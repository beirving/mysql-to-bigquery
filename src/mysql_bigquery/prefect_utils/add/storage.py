import os
from prefect import Task, Flow
from google.oauth2 import service_account

from mysql_bigquery.adapters.storage import StorageAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectAddServiceStorage(Task):
    def __init__(self, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"service-add-storage")
        super().__init__(**kwargs)

    def create_service_bucket(self, service: str):
        client = StorageAdapter(self.service_account)
        client.get_client()
        result = client.make_bucket(f"{service}-sync")
        if result:
            self.sd_logger.info(
                {'message': f'Created {service} Bucket'},
                {'class': 'PrefectAddServiceStorage', 'method': 'create_service_bucket'})
        else:
            self.sd_logger.warning(
                client.errors,
                {'class': 'PrefectAddServiceStorage', 'method': 'create_service_bucket'})
        return result


def storage_flow_runner(service: str):
    with Flow('Install Storage') as flow:
        installer = PrefectAddServiceStorage()
        installer.create_service_bucket(service)
    return flow.run()
