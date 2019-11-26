import os
import datetime
import pandas as pd
from typing import Union
from pandas import DataFrame
from prefect import Task, Flow
from google.oauth2 import service_account

from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.mysql import MySqlAdapter
from mysql_bigquery.adapters.pubsub import PubSubAdapter
from mysql_bigquery.adapters.storage import StorageAdapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class Data(Task):
    def __init__(self, service: str, **kwargs):
        self.chunk_size = 250000
        self.table = None
        self.watched_column = None
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = service_helpers.get_definitions(service)

        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"{self.definitions['service']}-etl")

        self.pub_sub_client = PubSubAdapter(self.service_account)
        self.pub_sub_client.get_subscriber()
        self.pub_sub_client.set_subscription(f"{self.definitions['service']}-etl-data")

        self.big_query_client = BigQueryAdapter(self.service_account)
        self.big_query_client.get_client()
        self.big_query_client.set_data_set_ref(self.definitions['data_set'])

        self.my_sql_client = MySqlAdapter(service)
        super().__init__(**kwargs)

    def check_message(self, message):
        if 'table' in message:
            self.table = message['table']
        else:
            error_message = 'Table was not included in the message'
            self.sd_logger.error(
                {'error': error_message},
                {'class': 'Data', 'method': 'check_message', 'table': self.table}
            )
            raise RuntimeError(error_message)

        if 'watched' in message:
            self.watched_column = message['watched']
        else:
            error_message = 'Watched was not included in the message'
            self.sd_logger.error(
                {'error': error_message},
                {'class': 'Data', 'method': 'check_message', 'table': self.table}
            )
            raise RuntimeError(error_message)

    def get_schema_from_big_query(self) -> bool:
        self.big_query_client.set_table_ref(self.table)
        table_check = self.big_query_client.check_table()
        if table_check:
            return self.big_query_client.get_schema()
        return table_check

    def last_updated_data(self) -> Union[str, bool]:
        query = f"SELECT MAX({self.watched_column}) as last_updated FROM {self.definitions['data_set']}.{self.table}"
        result = self.big_query_client.query(query)
        if result:
            for value in result:
                if value['last_updated']:
                    return value['last_updated']
                else:
                    # return none to allow all records to be pulled at the start
                    return None
        else:
            self.sd_logger.critical(
                self.big_query_client.errors,
                {'class': 'Data', 'method': 'last_updated_data', 'table': self.table}
            )
        return result

    def get_number_of_records_to_import(self, last_updated) -> Union[int, bool]:
        result = self.my_sql_client.count_items_to_sync(
            table=self.table,
            watched_column=self.watched_column,
            last_run=last_updated
        )
        if self.my_sql_client.errors:
            self.sd_logger.critical(
                self.my_sql_client.errors,
                {'class': 'Data', 'method': 'get_number_of_records_to_import', 'table': self.table}
            )
        return result

    def query_mysql_for_records(self, last_updated_date: str, limit: int, offset: int) -> Union[list, bool]:
        results = self.my_sql_client.get_records(
            table=self.table,
            watched_column=self.watched_column,
            last_run=last_updated_date,
            limit=limit,
            offset=offset
        )
        if not results:
            self.sd_logger.critical(
                self.my_sql_client.errors,
                {'class': 'Data', 'method': 'get_number_of_records_to_import', 'table': self.table}
            )
        return results

    def load_mysql_data_into_data_frame(self, data: list, schema: dict) -> Union[DataFrame, bool]:
        if len(data[0]) is not len(schema.keys()):
            self.sd_logger.critical(
                {
                    'message': "Schema and data length mismatch",
                    'schema_length': len(schema.keys()),
                    'data_length': len(data[0])
                },
                {'class': 'Data', 'method': 'load_mysql_data_into_data_frame', 'table': self.table}
            )
            return False
        df = pd.DataFrame.from_records(data, columns=schema.keys())
        del data
        return df

    def transform_data_frame_to_match_big_query_schema(
            self,
            data_frame: DataFrame,
            schema: dict
    ) -> Union[DataFrame, bool]:
        try:
            df = service_helpers.data_frame_to_schema(data_frame, schema)
        except ValueError as e:
            self.sd_logger.critical(
                {
                    'message': 'Error: {}'.format(e)
                },
                {
                    'class': 'Data',
                    'method': 'transform_data_frame_to_match_big_query_schema',
                    'table': self.table
                }
            )
            return False
        return df

    def append_data_frame_to_big_query(self, data_frame: DataFrame):
        result = self.big_query_client.upload_data_frame(data_frame)
        if result:
            message = f"table:{self.table} | Records written: {data_frame.shape[0]}"
            self.sd_logger.info(
                {'message': message},
                {'class': 'Data', 'method': 'append_data_frame_to_big_query', 'table': self.table}
            )

        else:
            self.sd_logger.critical(
                self.big_query_client.errors,
                {
                    'class': 'Data',
                    'method': 'append_data_frame_to_big_query',
                    'table': self.table
                }
            )
        return result

    def write_df_to_storage(self, df: DataFrame) -> bool:
        storage_client = StorageAdapter(self.service_account)
        storage_client.get_client()

        date_time_obj = datetime.datetime.utcnow()

        location = f'error/csv/{self.table}/{date_time_obj.strftime("%m-%d-%Y_%H:%M:%S")}_UTC'

        result = storage_client.write_string(
            bucket=self.definitions['service'] + '-etl',
            destination=location,
            string=df.to_csv(),
            encoding='text/csv'
        )
        if not result:
            self.sd_logger.error(
                storage_client.errors,
                {'class': 'Data', 'method': 'write_df_to_storage', 'table': self.table}
            )
            return False

        self.sd_logger.info(
            {'message': f"Failed CSV added to {location}"},
            {'class': 'Data', 'method': 'write_df_to_storage', 'table': self.table}
        )

        return result


def flow_runner(message):
    with Flow('Consume Data Job') as flow:
        consumer = Data(message['service'])
        consumer.check_message(message)
        get_schema_from_big_query = consumer.get_schema_from_big_query()
        if get_schema_from_big_query:
            last_updated_date = consumer.last_updated_data()
            records_to_update = consumer.get_number_of_records_to_import(last_updated_date)
            # we only run if we have new records to record
            if records_to_update > 0:
                # turn schema into a more usable list
                schema_simplified = service_helpers.convert_schema_to_dict(get_schema_from_big_query)

                # chunk data
                chunks = divmod(records_to_update, consumer.chunk_size)
                if chunks[0] == 0:
                    # if total number of records is less then chunk_size run just once
                    total_runs = 1
                else:
                    # account for remainder on modulus math
                    total_runs = chunks[0]+1

                current_run = 0
                offset = 0

                # Loop through all chunks
                while current_run < total_runs:
                    #  refresh the client connection on each import
                    consumer.big_query_client.get_client()
                    records = consumer.query_mysql_for_records(
                        last_updated_date=last_updated_date,
                        limit=consumer.chunk_size,
                        offset=offset
                    )

                    df = consumer.load_mysql_data_into_data_frame(
                        records,
                        schema_simplified
                    )

                    # break when we don't have a valid DataFrame to work with
                    if type(df) is bool:
                        break
                    df = consumer.transform_data_frame_to_match_big_query_schema(
                        df,
                        schema_simplified
                    )
                    # break when an unsupported data type is used
                    if type(df) is bool:
                        break
                    current_run += 1
                    offset += consumer.chunk_size
                    result = consumer.append_data_frame_to_big_query(df)
                    if result is False:
                        consumer.write_df_to_storage(df)
                    # memory management
                    del records
                    del df
    return flow.run()
