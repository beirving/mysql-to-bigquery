import time
import google
import concurrent
from typing import Union
from pandas import DataFrame
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions


def generate_params(params: list) -> list:
    output = []
    for param in params:
        output.append(
            bigquery.ScalarQueryParameter(
                param['variable'], param['type'], param['value']
            )
        )
    return output


class BigQueryAdapter:

    def __init__(self, credentials: service_account):
        self.service_account = credentials
        self.client = None
        self.data_set_ref = None
        self.table_ref = None
        self.table = None
        self.errors = {}

    def get_client(self) -> google.cloud.bigquery.client.Client:
        """Set up auth configured BigQuery client for use later
        :return: google.cloud.bigquery.client.Client
            Configured client using self.service_account auth
        """
        self.client = bigquery.Client(credentials=self.service_account, project=self.service_account.project_id)
        return self.client

    def set_data_set_ref(self, dataset: str):
        """Create a google.cloud.bigquery.dataset.DatasetReference object for use later
        :param dataset: str
            Name to use for generating a DatasetReference
        :return: nothing
        """
        self.check_dependencies()
        self.data_set_ref = self.client.dataset(dataset)

    def set_table_ref(self, table: str):
        """Create a google.cloud.bigquery.table.TableReference object for use later
        :param table: str
            Name to use for generating a TableReference
        :return: nothing
        """
        self.check_dependencies(True, True)
        self.table_ref = self.data_set_ref.table(table)

    def check_dependencies(self, client: bool = True, data_set: bool = False, table: bool = False):
        """Check dependencies for set class properties
        :param client: bool
        :param data_set: bool
        :param table: bool
        :return: Null
        """
        if client:
            if self.client is None:
                raise RuntimeError('client is not set')
        if data_set:
            if self.data_set_ref is None:
                raise RuntimeError('data_set_ref is not set')
        if table:
            if self.table_ref is None:
                raise RuntimeError('table_ref is not set')

    def check_dataset(self, data_set: str) -> bool:
        """Check to see if given dataset exists on the BigQuery client set up in get_client
        :dependencies:
            get_client
                self.client must be set prior to execution
        :note:
            Checking self.errors['check_dataset'] on False return
        :return: bool
            True on successful get_dataset, False on failed get_dataset
        """
        self.check_dependencies()
        try:
            response = self.client.get_dataset(data_set)
            if isinstance(response, google.cloud.bigquery.dataset.Dataset):
                return True
            else:
                self.errors['check_dataset'] = 'Response was not of the google.cloud.bigquery.dataset.Dataset type'
        except exceptions.GoogleAPICallError as e:
            self.errors['check_dataset'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def check_table(self) -> bool:
        """Check to see if self.table_ref table exists in self.data_set_ref dataset
        :dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
            set_table_ref
                self.table_ref must be set prior to execution
        :note:
            Checking self.errors['check_table'] on False return
            self.table is also set with google.cloud.bigquery.table.Table object for reference
        :return: bool
            True on successful get_table, False on failed get_table
        """
        self.check_dependencies(True, True, True)
        try:
            response = self.client.get_table(self.table_ref)
            if isinstance(response, google.cloud.bigquery.table.Table):
                self.table = response
                return True
            else:
                self.errors['check_table'] = 'Response was not of the google.cloud.bigquery.table.Table type'
        except exceptions.GoogleAPICallError as e:
            self.errors['check_table'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def create_data_set(self, location: str = 'US') -> bool:
        """Create dataset in configured Google Cloud Platform account
        :dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
        :note:
            Checking self.errors['create_data_set'] on False return
        :return: bool
            True on successful create_dataset, False on failed create_dataset
        """
        self.check_dependencies(True, True)

        if self.check_dataset(self.data_set_ref):
            self.errors['create_data_set'] = f"dataset '{self.data_set_ref}' already exists"
            return False
        # reset errors check_dataset error from not finding the dataset
        del self.errors['check_dataset']

        self.data_set_ref.location = location

        try:
            result = self.client.create_dataset(self.data_set_ref)
            if isinstance(result, google.cloud.bigquery.dataset.Dataset):
                return True
        except exceptions.GoogleAPICallError as e:
            self.errors['create_data_set'] = 'GoogleAPICallError: {}'.format(e)

        return False

    def create_table(self, schema: list, overwrite: bool = False) -> bool:
        """Create table inside configured dataset on configured Google Cloud Platform account
        :dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
            set_table_ref
                self.table_ref must be set prior to execution
        :note:
            Checking self.errors['create_table'] on False return
        :return: bool
            True on successful create_table, False on failed create_table
        """
        self.check_dependencies(True, True, True)

        table = bigquery.Table(self.table_ref, schema=schema)
        try:
            response = self.client.create_table(table=table, exists_ok=overwrite)
            if isinstance(response, google.cloud.bigquery.table.Table):
                return True
            else:
                self.errors['create_table'] = 'Response was not of the google.cloud.bigquery.table.Table type'
        except exceptions.GoogleAPICallError as e:
            self.errors['create_table'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def delete_table(self):
        """ Delete Table
        dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
            set_table_ref
                self.table_ref must be set prior to execution
        :note:
            Checking self.errors['export_table_to_storage'] on empty list returned
        :return: bool
            True on successful extract_table, False on failed extract_table
        """
        self.check_dependencies(True, True, True)

        try:
            self.client.delete_table(table=self.table_ref)
            # no return value from delete_table only raises on errors
            return True
        except exceptions.GoogleAPICallError as e:
            self.errors['delete_table'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def get_schema(self) -> list:
        """Query for the Schema of self.table_ref
        :dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
            set_table_ref
                self.table_ref must be set prior to execution
        :note:
            Checking self.errors['get_schema'] on empty list returned
            If check_table is run prior a cached version of the set table.ref will be returned
        :return: list
            List of BigQuery Columns on successful query
            Empty list is returned on failure
        """
        self.check_dependencies(True, True, True)

        # if table is already set return the cached version
        if self.table:
            return self.table.schema
        try:
            response = self.check_table()
            if response:
                return self.table.schema
        except exceptions.GoogleAPICallError as e:
            self.errors['get_schema'] = 'GoogleAPICallError: {}'.format(e)
        except concurrent.futures.TimeoutError as e:
            self.errors['get_schema'] = 'TimeoutError: {}'.format(e)
        return []

    def query(self, query: str, params: list = None) -> Union[google.cloud.bigquery.table.RowIterator, bool]:
        """Execute query on configured Google cloud Platform BigQuery Account
        :param query: str
            Query to be executed
        :param params: list
            variable query replacement
            list of Dictionaries
                {'variable': 'variable', 'type': 'type', 'value': 'value'}
        :dependencies:
            get_client
                self.client must be set prior to execution
        :note:
            Checking self.errors['query'] on empty list returned
        :return: google.cloud.bigquery.table.RowIterator
            RowIterator of BigQuery data returned from given query
            False is returned on failure
        """
        self.check_dependencies(True)

        job_config = bigquery.QueryJobConfig()

        if params is not None:
            job_config.query_parameters = generate_params(params)
        try:
            job = self.client.query(
                query,
                job_config=job_config
            )
            if isinstance(job, google.cloud.bigquery.job.QueryJob):
                return job.result()
            else:
                self.errors['query'] = 'job is not instance of google.cloud.bigquery.job.QueryJob'
        except exceptions.GoogleAPICallError as e:
            self.errors['query'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def upload_data_frame(self, data_frame: DataFrame, if_exists='append') -> bool:
        """Given
        :param data_frame: DataFrame
            Pandas DataFrame to be uploaded
        :param if_exists: str (optional)
            Optional Method for insertion of data
                fail
                    If table exists, do nothing.
                replace
                    If table exists, drop it, recreate it, and insert data.
                append (default)
                    If table exists, insert data. Create if does not exist.

        :dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
            set_table_ref
                self.table_ref must be set prior to execution
        :note:
            Checking self.errors['upload_data_frame'] on empty list returned
        :return: bool
            True on successful to_gbq, False on failed to_gbq
        """

        self.check_dependencies(True, True, True)

        try:
            data_frame.to_gbq(
                f"{self.table_ref.dataset_id}.{self.table_ref.table_id}",
                self.service_account.project_id,
                credentials=self.service_account,
                if_exists=if_exists
            )
            return True
        except Exception as e:
            print(e)
            self.errors['upload_data_frame'] = 'Exception: {}'.format(e)
        return False

    def copy_table(
            self,
            copy_table: google.cloud.bigquery.table.TableReference,
            destination_table: google.cloud.bigquery.table.TableReference
    ) -> bool:
        """ Create a copy of an existing table
        :param copy_table: google.cloud.bigquery.table.TableReference
            Table to be copied
        :param destination_table: google.cloud.bigquery.table.TableReference
            Destination of copy
        dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
        :note:
            Checking self.errors['copy_table'] on empty list returned
        :return: bool
            True on successful copy_table, False on failed copy_table
        """
        self.check_dependencies(True, True)

        try:
            copy_job = self.client.copy_table(sources=copy_table, destination=destination_table)
            if isinstance(copy_job, google.cloud.bigquery.job.CopyJob):
                while copy_job.done() is False:
                    time.sleep(1)
                return copy_job.done()

        except exceptions.GoogleAPICallError as e:
            self.errors['copy_table'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def export_table_to_storage(
            self,
            table: google.cloud.bigquery.table.TableReference,
            destination: str
    ) -> bool:
        """ Export table to GCP Storage
        :param table: google.cloud.bigquery.table.TableReference
            Table to be copied
        :param destination: str
            Destination of export
        dependencies:
            get_client
                self.client must be set prior to execution
            data_set_ref
                self.data_set_ref must be set prior to execution
        :note:
            Checking self.errors['export_table_to_storage'] on empty list returned
        :return: bool
            True on successful extract_table, False on failed extract_table
        """
        self.check_dependencies(True, True)

        try:
            extract_job = self.client.extract_table(source=table, destination_uris=destination)
            if isinstance(extract_job, google.cloud.bigquery.job.ExtractJob):
                while extract_job.done() is False:
                    time.sleep(1)
                return extract_job.done()
        except exceptions.GoogleAPICallError as e:
            self.errors['export_table_to_storage'] = 'GoogleAPICallError: {}'.format(e)
        return False
