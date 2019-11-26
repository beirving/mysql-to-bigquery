import os
import json
from typing import Union
from datetime import datetime
from prefect import Task, Flow
from google.oauth2 import service_account

from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.mysql import MySqlAdapter
from mysql_bigquery.adapters.pubsub import PubSubAdapter
from mysql_bigquery.adapters.storage import StorageAdapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class Schema(Task):
    def __init__(self, service: str,  **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = service_helpers.get_definitions(service)

        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"{self.definitions['service']}-etl")

        self.pub_sub_client = PubSubAdapter(self.service_account)
        self.pub_sub_client.get_subscriber()
        self.pub_sub_client.set_subscription(f"{self.definitions['service']}-etl-schema")

        self.big_query_client = BigQueryAdapter(self.service_account)
        self.big_query_client.get_client()
        self.big_query_client.set_data_set_ref(self.definitions['data_set'])
        super().__init__(**kwargs)

    def get_mysql_schema(self, table: str) -> Union[list, bool]:
        mysql_client = MySqlAdapter(self.definitions['service'])
        columns = mysql_client.mysql_table_definition(table)
        if not columns:
            self.sd_logger.error(
                mysql_client.errors,
                {'class': 'Schema', 'method': 'get_mysql_schema', 'table': table}
            )
            return False
        return columns

    @staticmethod
    def organized_mysql_schema(schema: list) -> list:
        organized_column_data = []
        for column in schema:
            organized_column_data.append(service_helpers.label_mysql_table_definitions(column))
        return organized_column_data

    @staticmethod
    def convert_mysql_to_big_query_schema(schema: list) -> list:
        return service_helpers.generate_bq_schema_from_mysql(schema)

    def store_mysql_schema(self, schema: list, table: str) -> bool:
        encoded_schema = json.dumps(schema)
        storage_client = StorageAdapter(self.service_account)
        storage_client.get_client()

        date_time_obj = datetime.utcnow()

        result = storage_client.write_string(
            bucket=self.definitions['service']+'-etl',
            destination=f'schema/{table}/{date_time_obj.strftime("%m-%d-%Y_%H:%M:%S")}_UTC',
            string=encoded_schema
        )
        if not result:
            self.sd_logger.error(
                storage_client.errors,
                {'class': 'Schema', 'method': 'store_mysql_schema', 'table': table}
            )
            return False

        return result

    def check_table_exists(self, table):
        self.big_query_client.set_table_ref(table)
        return self.big_query_client.check_table()

    def get_current_schema(self, table: str):
        self.big_query_client.set_table_ref(table)
        return self.big_query_client.get_schema()

    @staticmethod
    def compare_schema(new_schema, current_schema) -> bool:
        # first check is total number of items
        if len(new_schema) is not len(current_schema):
            return False
        # compare column names and types
        schema_matches = True
        for x in range(len(new_schema)):
            exists_in_current = False
            if current_schema[x].name == new_schema[x].name:
                if current_schema[x].field_type == new_schema[x].field_type:
                    exists_in_current = True
            else:
                exists_in_current = True

            if not exists_in_current:
                schema_matches = False
        return schema_matches

    def create_table(self, table: str, schema: list) -> bool:
        self.big_query_client.set_table_ref(table)
        result = self.big_query_client.create_table(schema=schema, overwrite=True)
        if result:
            self.sd_logger.info(
                {'message': f"Table {table} Created"},
                {'class': 'Schema', 'method': 'create_table', 'table': table}
            )
        else:
            self.sd_logger.error(
                self.big_query_client.errors,
                {'class': 'Schema', 'method': 'create_table', 'table': table}
            )
        return result

    @staticmethod
    def acknowledge_message(message):
        message.ack()

    def copy_bq_table(self, table: str):
        self.big_query_client.set_table_ref(table)
        copy_table_ref = self.big_query_client.table_ref

        date_time_obj = datetime.utcnow()
        destination_str = f'{table}_{date_time_obj.strftime("%m_%d_%Y")}'
        self.big_query_client.set_table_ref(destination_str)
        destination_table_ref = self.big_query_client.table_ref

        result = self.big_query_client.copy_table(
            copy_table=copy_table_ref,
            destination_table=destination_table_ref
        )
        if result:
            self.sd_logger.warning(
                {'message': f"Table {table} copied to {destination_str}"},
                {'class': 'Schema', 'method': 'copy_bq_table', 'table': table}
            )
        else:
            self.sd_logger.error(
                self.big_query_client.errors,
                {'class': 'Schema', 'method': 'copy_bq_table', 'table': table}
            )
        return destination_str

    def backup_table_to_storage(self, table):
        self.big_query_client.set_table_ref(table)
        copy_table_ref = self.big_query_client.table_ref

        date_time_obj = datetime.utcnow()
        destination = f'gs://{self.definitions["service"]}-etl/data/{table}/{date_time_obj.strftime("%m-%d-%Y_%H:%M:%S")}_UTC_*.avro'
        result = self.big_query_client.export_table_to_storage(
            table=copy_table_ref,
            destination=destination
        )
        if result:
            self.sd_logger.info(
                {'message': f"Table {table} exported to {destination}"},
                {'class': 'Schema', 'method': 'backup_table_to_storage'}
            )
        else:
            self.sd_logger.error(
                self.big_query_client.errors,
                {'class': 'Schema', 'method': 'backup_table_to_storage'}
            )
        return result

    def delete_table(self, table):
        self.big_query_client.set_table_ref(table)
        result = self.big_query_client.delete_table()
        if result:
            self.sd_logger.info(
                {'message': f"Table {table} deleted"},
                {'class': 'Schema', 'method': 'delete_table'}
            )
        else:
            self.sd_logger.error(
                self.big_query_client.errors,
                {'class': 'Schema', 'method': 'delete_table'}
            )
        return result


def flow_runner(message: dict):
    with Flow('Consume Schema Job') as flow:
        consumer = Schema(message['service'])
        target_mysql_schema = consumer.get_mysql_schema(message['table'])
        organized_mysql_schema = consumer.organized_mysql_schema(target_mysql_schema)
        mysql_converted_schema = consumer.convert_mysql_to_big_query_schema(organized_mysql_schema)

        # first run logic
        table_exists = consumer.check_table_exists(message['table'])
        if table_exists:
            # logic to replace existing table if changes in schema have happened
            big_query_schema = consumer.get_current_schema(message['table'])
            compare_result = consumer.compare_schema(mysql_converted_schema, big_query_schema)

            # schema has not changed
            if not compare_result or message['flush'] is True:
                # schema has changed
                consumer.store_mysql_schema(organized_mysql_schema, message['table'])
                copy_bq_table = consumer.copy_bq_table(message['table'])
                consumer.delete_table(message['table'])
                consumer.create_table(message['table'], mysql_converted_schema)
                consumer.backup_table_to_storage(copy_bq_table)
                consumer.delete_table(copy_bq_table)
        else:
            # create the table when does not exist
            consumer.store_mysql_schema(organized_mysql_schema, message['table'])
            consumer.create_table(message['table'], mysql_converted_schema)
    return flow.run()
