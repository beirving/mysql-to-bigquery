import os
from prefect import Task, Flow
from google.cloud import bigquery
from google.oauth2 import service_account

from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectInstallBigQuery(Task):
    def __init__(self, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"install-big-query")
        super().__init__(**kwargs)

    def create_client(self) -> BigQueryAdapter:
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        return big_query_client

    def create_config_data_set(self, big_query_client: BigQueryAdapter) -> bool:
        # return True if dataset already exists
        if big_query_client.check_dataset('mysql_sync'):
            return True
        big_query_client.set_data_set_ref('mysql_sync')
        result = big_query_client.create_data_set()
        if result:
            self.sd_logger.info(
                {'message': f"Created mysql_sync Data Set"},
                {'class': 'PrefectInstallBigQuery', 'method': 'create_data_set'})
        else:
            self.sd_logger.warning(
                big_query_client.errors,
                {'class': 'PrefectInstallBigQuery', 'method': 'create_data_set'}
            )
        return result

    def create_config_table(self, big_query_client: BigQueryAdapter):
        big_query_client.set_data_set_ref('mysql_sync')
        big_query_client.set_table_ref('data_sources')
        # return true if table already exists
        if big_query_client.check_table():
            return True

        schema = [
            bigquery.SchemaField(
                'service',
                'STRING',
                description="Service Name"
            ),
            bigquery.SchemaField(
                'data_set',
                'STRING',
                description="Big Query Data Set"
            ),
            bigquery.SchemaField(
                'host',
                'STRING',
                description="MySQL host connection"
            ),
            bigquery.SchemaField(
                'user',
                'STRING',
                description="MySQL connection user"
            ),
            bigquery.SchemaField(
                'password',
                'STRING',
                description="MySQL connection password"
            ),
            bigquery.SchemaField(
                'database',
                'STRING',
                description="MySQL Database"
            ),
            bigquery.SchemaField(
                'last_run',
                'TIMESTAMP',
                description="Last produce date"
            ),
        ]
        result = big_query_client.create_table(schema)
        if result:
            self.sd_logger.info(
                {'message': f"Created Config table"},
                {'class': 'PrefectInstallBigQuery', 'method': 'create_config_table'}
            )
        else:
            self.sd_logger.warning(
                big_query_client.errors,
                {'class': 'PrefectInstallBigQuery', 'method': 'create_config_table'}
            )
        return result


def big_query_flow_runner():
    with Flow('Install BigQuery') as flow:
        installer = PrefectInstallBigQuery()
        client = installer.create_client()
        installer.create_config_data_set(client)
        installer.create_config_table(client)

    return flow.run()
