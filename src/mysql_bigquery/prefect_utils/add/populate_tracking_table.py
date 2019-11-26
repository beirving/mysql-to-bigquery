import os
import pandas as pd
from typing import Union
from prefect import Task, Flow
from google.oauth2 import service_account

from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.mysql import MySqlAdapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PopulateTrackingTable(Task):
    def __init__(self, service: str, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = service_helpers.get_definitions(service)
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"service-add-populate-tracking")
        self.mysql_client = MySqlAdapter(service)
        super().__init__(**kwargs)

    def create_client(self) -> BigQueryAdapter:
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        return big_query_client

    def define_tables(self) -> Union[list, bool]:
        table_definitions = self.mysql_client.mysql_table_definition()
        if not isinstance(table_definitions, list):
            self.sd_logger.error(
                self.mysql_client.errors,
                {'class': 'PopulateTrackingTable', 'method': 'define_tables'}
            )
            return False
        return table_definitions

    @staticmethod
    def organize_tables(columns: list) -> dict:
        tables = {}
        for column in columns:
            if not column['table'] in tables:
                tables[column['table']] = []
            tables[column['table']].append(service_helpers.label_mysql_table_definitions(column))
        return tables

    @staticmethod
    def format_for_big_query(tables: dict) -> list:
        output = []
        for table, data in tables.items():
            item = {
                'table': table,
                'watched': service_helpers.guess_watched(data),
                'primary_id': service_helpers.guess_pk(data),
            }

            if item['watched'] is None or item['primary_id'] is None:
                item['synchronize'] = False
            else:
                item['synchronize'] = True
            output.append(item)
        return output

    @staticmethod
    def convert_to_data_frame(data: list) -> pd.DataFrame:
        df = pd.DataFrame(data=data)
        return df

    def write_to_big_query(self, data: pd.DataFrame) -> bool:
        bq = BigQueryAdapter(self.service_account)
        bq.get_client()
        bq.set_data_set_ref(self.definitions['data_set'])
        bq.set_table_ref('sync_tracking_table')
        result = bq.upload_data_frame(data, 'replace')
        if result:
            self.sd_logger.info(
                {"Tracking table populated": data.count(axis=0).to_dict()},
                {'class': 'PopulateTrackingTable', 'method': 'write_to_big_query'}
            )
        else:
            self.sd_logger.error(
                bq.errors,
                {'class': 'PopulateTrackingTable', 'method': 'write_to_big_query'}
            )
        return result


def populate_tracking_table_flow_runner(service):
    with Flow('Populate Tracking Table') as flow:
        pptt = PopulateTrackingTable(service)
        pptt.create_client()
        defined_tables = pptt.define_tables()
        if defined_tables:
            organize_tables = pptt.organize_tables(defined_tables)
            format_for_big_query = pptt.format_for_big_query(organize_tables)
            convert_to_data_frame = pptt.convert_to_data_frame(format_for_big_query)
            pptt.write_to_big_query(convert_to_data_frame)

    return flow.run()
