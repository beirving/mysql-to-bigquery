import os
from prefect import Task, Flow
from google.cloud import bigquery
from google.oauth2 import service_account

from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectAddServiceBigQuery(Task):
    def __init__(self, service: str, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = service_helpers.get_definitions(service)
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"service-add-big-query")
        super().__init__(**kwargs)

    def create_client(self) -> BigQueryAdapter:
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        return big_query_client

    def create_data_set(self, big_query_client: BigQueryAdapter) -> bool:
        # return True if dataset already exists
        if big_query_client.check_dataset(self.definitions['data_set']):
            return True
        big_query_client.set_data_set_ref(self.definitions['data_set'])
        result = big_query_client.create_data_set()
        if result:
            self.sd_logger.info(
                {'message': f"Created {self.definitions['data_set']} Data Set"},
                {'class': 'PrefectAddServiceBigQuery', 'method': 'create_data_set'})
        else:
            self.sd_logger.warning(
                big_query_client.errors,
                {'class': 'PrefectAddServiceBigQuery', 'method': 'create_data_set'}
            )
        return result

    def create_tracking_table(self, big_query_client: BigQueryAdapter):
        big_query_client.set_data_set_ref(self.definitions['data_set'])
        big_query_client.set_table_ref('sync_tracking_table')
        # return true if table already exists
        if big_query_client.check_table():
            return True
        schema = [
            bigquery.SchemaField(
                'table',
                'STRING',
                description="Tracked Table Name"
            ),
            bigquery.SchemaField(
                'watched',
                'STRING',
                description="Column to watch to minimize the number of records loaded per sync"
            ),
            bigquery.SchemaField(
                'primary_id',
                'STRING',
                description="Primary Id Column(s)"
            ),
            bigquery.SchemaField(
                'synchronize',
                'BOOLEAN',
                description="Flag to Synchronize the table"
            )
        ]
        result = big_query_client.create_table(schema)
        if result:
            self.sd_logger.info(
                {'message': f"Created tracking table"},
                {'class': 'PrefectAddServiceBigQuery', 'method': 'create_tracking_table'}
            )
        else:
            self.sd_logger.warning(
                big_query_client.errors,
                {'class': 'PrefectAddServiceBigQuery', 'method': 'create_tracking_table'}
            )
        return result


def big_query_flow_runner(service: str):
    with Flow('Add Service BigQuery') as flow:
        installer = PrefectAddServiceBigQuery(service)
        client = installer.create_client()
        installer.create_data_set(client)
        installer.create_tracking_table(client)

    return flow.run()
