import os
from typing import Union
import google
from google.cloud import storage
from google.oauth2 import service_account
from google.api_core import exceptions


class StorageAdapter:
    def __init__(self, credentials: service_account):
        self.service_account = credentials
        self.client = None
        self.errors = {}

    def get_client(self) -> google.cloud.storage.client.Client:
        if self.client is None:
            self.client = storage.Client(credentials=self.service_account, project=self.service_account.project_id)
        return self.client

    def make_bucket(self, bucket_name: str) -> bool:
        if self.client is None:
            raise RuntimeError('Storage Client is not configured')
        try:
            result = self.client.create_bucket(bucket_name)
            if result.exists():
                return True
        except exceptions.GoogleAPICallError as e:
            self.errors['create_bucket'] = 'GoogleAPICallError: {}'.format(e)

        return False

    def write_file(self, bucket: str, destination: str, file_name_location: str, delete: bool = False) -> bool:
        if self.client is None:
            raise RuntimeError('Storage Client is not configured')

        try:
            bucket = self.client.get_bucket(bucket)

            blob = bucket.blob(destination)

            blob.upload_from_filename(file_name_location)

            # there is no return from upload_from_filename
            # if there is no exception we can assume successful file write
            return True
        except exceptions.GoogleAPICallError as e:
            self.errors['write_object'] = 'GoogleAPICallError: {}'.format(e)
        except FileNotFoundError as e:
            self.errors['write_object'] = 'FileNotFoundError: {}'.format(e)
        finally:
            if delete:
                if os.path.exists(file_name_location):
                    os.remove(file_name_location)

        return False

    def write_string(self, bucket: str, destination: str, string: str, encoding: str = None) -> bool:
        if self.client is None:
            raise RuntimeError('Storage Client is not configured')

        try:
            bucket = self.client.get_bucket(bucket)

            blob = bucket.blob(destination)

            blob.upload_from_string(string, encoding)

            # there is no return from upload_from_filename
            # if there is no exception we can assume successful file write
            return True
        except exceptions.GoogleAPICallError as e:
            self.errors['write_object'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def get_file_as_string(self, bucket: str, file: str) -> Union[bool, str]:
        if self.client is None:
            raise RuntimeError('Storage Client is not configured')
        try:
            bucket = self.client.get_bucket(bucket)

            blob = bucket.blob(f"{file}")

            return blob.download_as_string()
        except exceptions.GoogleAPICallError as e:
            self.errors['write_object'] = 'GoogleAPICallError: {}'.format(e)
        return False
