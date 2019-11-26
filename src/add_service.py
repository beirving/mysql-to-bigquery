import os
import re
import pandas as pd
from prefect import Task, Flow
from cryptography.fernet import Fernet
from google.oauth2 import service_account
from mysql_bigquery.adapters.storage import StorageAdapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter
import mysql_bigquery.prefect_utils.add.storage as storage_add
import mysql_bigquery.prefect_utils.add.big_query as big_query_add
import mysql_bigquery.prefect_utils.add.populate_tracking_table as populate


class AddService(Task):
    def __init__(self):
        self.service = None
        self.data_set = None
        self.host = None
        self.user = None
        self.password = None
        self.database = None

        self.key = None

        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )

        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"add-service")

    def generate_key(self):
        self.key = Fernet.generate_key()

    def set_attribute(self, attribute: str, value: str, encode: bool = False):
        if encode:
            f = Fernet(self.key)
            encoded = f.encrypt(bytes(value, encoding='utf-8'))
            value = str(encoded, 'utf-8')

        setattr(self, attribute, value)

    def save_key(self):
        storage_client = StorageAdapter(self.service_account)
        storage_client.get_client()
        result = storage_client.write_string(
            bucket='mysql_sync_keys',
            destination=f"{self.service}",
            string=self.key
        )

        if result is False:
            raise RuntimeError(storage_client.errors)

    def save_record(self):
        record = {
            'service': [self.service],
            'data_set': [self.data_set],
            'host': [self.host],
            'user': [self.user],
            'password': [self.password],
            'database': [self.database]
        }
        df = pd.DataFrame(record)
        big_query = BigQueryAdapter(self.service_account)
        big_query.get_client()
        big_query.set_data_set_ref('mysql_sync')
        big_query.set_table_ref('data_sources')
        result = big_query.upload_data_frame(df)
        if result is False:
            self.sd_logger.error(
                big_query.errors,
                {'class': 'AddService', 'method': 'save_record'}
            )

    def storage(self):
        return storage_add.storage_flow_runner(self.service)

    def big_query(self):
        return big_query_add.big_query_flow_runner(self.service)

    def populate_tracking(self):
        return populate.populate_tracking_table_flow_runner(self.service)

    def run_set_up(self):
        service = input('Service Name: ')
        service = re.sub(r"[^\w]|_", '-', service)
        self.set_attribute('service', service)

        data_set = input('Data Set: ')
        data_set = re.sub(r"[^\w]|-", '_', data_set)
        self.set_attribute('data_set', data_set)

        self.set_attribute('host', input('MySQL Host Connection: '), True)
        self.set_attribute('user', input('MySQL Connection User: '), True)
        self.set_attribute('password', input('MySQL Connection Password: '), True)
        self.set_attribute('database', input('MySQL Database: '))


with Flow('Add Service') as flow:
    add = AddService()
    add.generate_key()
    add.run_set_up()
    add.save_key()
    add.save_record()
    add.storage()
    add.big_query()
    add.populate_tracking()

state = flow.run()
