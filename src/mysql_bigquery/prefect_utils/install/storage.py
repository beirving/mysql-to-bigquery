import os
from prefect import Task, Flow
from google.oauth2 import service_account
from mysql_bigquery.adapters.storage import StorageAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectInstallStorage(Task):
    def __init__(self, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"storage-install-mysql-sync")
        super().__init__(**kwargs)

    def create_config_bucket(self):
        client = StorageAdapter(self.service_account)
        client.get_client()
        result = client.make_bucket('mysql_sync_keys')
        if result:
            self.sd_logger.info(
                {'message': f'Created mysql_sync_keys Bucket'},
                {'class': 'PrefectInstallStorage', 'method': 'create_config_bucket'})
        else:
            self.sd_logger.warning(
                client.errors,
                {'class': 'PrefectInstallStorage', 'method': 'create_config_bucket'})
        return result


def storage_flow_runner():
    with Flow('Install Storage') as flow:
        installer = PrefectInstallStorage()
        installer.create_config_bucket()
    return flow.run()
