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
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class Audit(Task):
    def __init__(self, service: str, **kwargs):
        self.table = None
        self.watched_column = None
        self.primary_id = None
        self.sample_size = 1000
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = service_helpers.get_definitions(service)

        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"{self.definitions['service']}-etl")

        self.pub_sub_client = PubSubAdapter(self.service_account)
        self.pub_sub_client.get_subscriber()
        self.pub_sub_client.set_subscription(f"{self.definitions['service']}-etl")

        self.big_query_client = BigQueryAdapter(self.service_account)
        self.big_query_client.get_client()
        self.big_query_client.set_data_set_ref(self.definitions['data_set'])

        self.my_sql_client = MySqlAdapter()
        super().__init__(**kwargs)

    def extract_message_values(self, message: dict) -> bool:
        if self.table is None:
            if 'table' in message:
                self.table = message['table']
            else:
                self.sd_logger.error(
                    {'error': 'Key: table was not found in message', 'data': message},
                    {'class': 'Audit', 'method': 'extract_message_values'}
                )
                return False

            if 'watched' in message:
                self.watched_column = message['watched']
            else:
                self.sd_logger.error(
                    {'error': 'Key: watched was not found in message', 'data': message},
                    {'class': 'Audit', 'method': 'extract_message_values', "table": self.table}
                )
                return False

            if 'primary_id' in message:
                self.primary_id = message['primary_id']
            else:
                self.sd_logger.error(
                    {'error': 'Key: primary_id was not found in message', 'data': message},
                    {'class': 'Audit', 'method': 'extract_message_values', "table": self.table}
                )
                return False
        return True

    def big_query_last_updated_data(self) -> Union[datetime.datetime, bool]:
        query = f"SELECT MAX({self.watched_column}) as last_updated FROM {self.definitions['data_set']}.{self.table}"
        result = self.big_query_client.query(query)
        if result:
            for value in result:
                if value.last_updated:
                    return value.last_updated
                else:
                    # use string of the start of unix time as the default time
                    return False
        else:
            self.sd_logger.critical(
                self.big_query_client.errors,
                {'class': 'Audit', 'method': 'big_query_last_updated_data', 'table': self.table}
            )
        return result

    def get_total_distinct_mysql(self, last_updated: datetime.datetime) -> Union[int, bool]:
        if last_updated:
            records = self.my_sql_client.count_distinct(
                table=self.table,
                index=self.primary_id,
                watched_column=self.watched_column,
                last_updated=last_updated
            )
        else:
            records = self.my_sql_client.count_distinct(
                table=self.table,
                index=self.primary_id
            )
        if records is False:
            self.sd_logger.error(
                {'error': self.my_sql_client.errors},
                {'class': 'Audit', 'method': 'get_total_distinct_mysql', "table": self.table}
            )
        return records

    def get_total_distinct_big_query(self) -> int:
        query = [
            "SELECT COUNT(*) as total_count",
            "FROM (",
            f"select DISTINCT {self.primary_id}",
            f"FROM {self.definitions['data_set']}.{self.table}",
            ")"
        ]

        result = self.big_query_client.query(' '.join(query))

        if not result:
            self.sd_logger.error(
                {'error': self.big_query_client.errors},
                {'class': 'Audit', 'method': 'get_total_distinct_big_query', "table": self.table}
            )
            return False

        for item in result:
            return item.total_count

    def audit_from_totals(self, total_mysql: int, total_big_query: int) -> bool:
        # check if the totals are the same
        if total_mysql == total_big_query:
            return True
        # big_query total will be lower than the mysql most of the time
        # allow it is be
        passing_total = int(round((total_mysql*.99), 0))
        if total_big_query >= passing_total:
            return True

        self.sd_logger.info(
            {
                'message': f"Audit totals did not match: {self.table}",
                "data": {
                    "MySQL Total": total_mysql,
                    "Big Query Total": total_big_query,
                    "passing total": passing_total
                }
            },
            {'class': 'Audit', 'method': 'audit_from_totals', "table": self.table}
        )
        return False

    def get_random_mysql(self, last_run: int = None) -> Union[list, bool]:
        result = self.my_sql_client.get_random_records(
            table=self.table,
            limit=self.sample_size,
            index=self.primary_id,
            watched=self.watched_column,
            last_updated=last_run
        )
        if result is not False:
            return result

        self.sd_logger.error(
            {'error': self.my_sql_client.errors},
            {'class': 'Audit', 'method': 'get_random_mysql', "table": self.table}
        )
        return False

    def get_big_query_records_from_sample(self, sample: list) -> DataFrame:
        primary_ids = self.primary_id.split(',')
        or_statements = []
        # get the same items from the mysql random query in big query
        for item in sample:
            pk_statements = []
            for key in primary_ids:
                if type(item[key]) is int:
                    pk_statements.append(f"`{key}` = {item[key]}")
                else:
                    pk_statements.append(f"`{key}` = '{item[key]}'")
            if type(item[self.watched_column]) is int:
                watched_condition = f"AND `{self.watched_column}` = {item[self.watched_column]}"
            else:
                if item[self.watched_column] is None:
                    watched_condition = f"AND `{self.watched_column}` IS NULL"
                else:
                    watched_condition = f"AND `{self.watched_column}` = '{item[self.watched_column]}'"
            or_statements.append(f"({' AND '.join(pk_statements)} {watched_condition})")
        query =[
            f"SELECT *",
            f"FROM {self.definitions['data_set']}.{self.table}",
            f"WHERE {' OR '.join(or_statements)}",
            "ORDER BY"
        ]
        order_by =[]
        for key in primary_ids:
            order_by.append(f"{key} ASC")
        query.append(', '.join(order_by))

        result = self.big_query_client.query(' '.join(query))

        if not result:
            self.sd_logger.error(
                {'error': self.big_query_client.errors},
                {'class': 'Audit', 'method': 'get_big_query_records_from_sample', "table": self.table}
            )
            return False
        output = result.to_dataframe()
        return output

    def convert_mysql_to_data_frame(self, mysql_data: list) -> Union[DataFrame, bool]:
        self.big_query_client.set_table_ref(self.table)
        bq_schema = self.big_query_client.get_schema()
        schema = service_helpers.convert_schema_to_dict(bq_schema)

        if len(mysql_data[0]) is not len(schema.keys()):
            self.sd_logger.critical(
                {
                    'message': "Schema and data length mismatch",
                    'schema_length': len(schema.keys()),
                    'data_length': len(mysql_data[0])
                },
                {'class': 'Audit', 'method': 'convert_mysql_to_data_frame', 'table': self.table}
            )
            return False
        data_frame = pd.DataFrame.from_records(mysql_data, columns=schema.keys())

        try:
            df = service_helpers.data_frame_to_schema(data_frame, schema, utc=True)
        except ValueError as e:
            self.sd_logger.critical(
                {
                    'message': 'Error: {}'.format(e)
                },
                {
                    'class': 'Audit',
                    'method': 'transform_data_frame_to_match_big_query_schema',
                    'table': self.table
                }
            )
            return False
        return df

    def audit_from_sample(self, mysql_data: DataFrame, big_query_data: DataFrame) -> bool:
        # test if data frames are equal -  this will fail in most cases to do the poor data quality from sources
        result = mysql_data.equals(big_query_data)
        if result:
            return True
        # check if the number of returned rows are the same
        # this check is useful because of the strictness of the queries that generate these DataFrames
        if mysql_data.shape[0] == big_query_data.shape[0]:
            return True

        self.sd_logger.info(
            {
                'message': f"Audit Failure: {self.table}",
                'DataFrame.equals': result,
                'mysql.shape': mysql_data.shape[0],
                'big_query.shape': big_query_data.shape[0]
            },
            {
                'class': 'Audit',
                'method': 'audit_from_sample',
                'table': self.table
            }
        )
        return False


def flow_runner(message: dict):
    with Flow('Consume Audit Job') as flow:
        consumer = Audit(message['service'])
        consumer.extract_message_values(message)

        last_updated = consumer.big_query_last_updated_data()

        # get count values for distinct entries
        distinct_mysql = consumer.get_total_distinct_mysql(last_updated)
        distinct_big_query = consumer.get_total_distinct_big_query()

        totals_audit = consumer.audit_from_totals(total_mysql=distinct_mysql, total_big_query=distinct_big_query)
        if totals_audit is False:
            return False

        # gather random sample from mysql
        mysql_sample = consumer.get_random_mysql(last_updated)

        # if MySQL is empty
        if len(mysql_sample) is 0:
            return True
        if mysql_sample is False:
            return False
        mysql_df = consumer.convert_mysql_to_data_frame(mysql_sample)
        if mysql_df is False:
            return False

        bq_df = consumer.get_big_query_records_from_sample(mysql_sample)

        return consumer.audit_from_sample(mysql_data=mysql_df, big_query_data=bq_df)
