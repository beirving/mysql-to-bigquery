import google
from google.cloud import logging
from google.api_core import exceptions
from google.oauth2 import service_account


class StackDriverAdapter:

    def __init__(self,  credentials: service_account):
        self.service_account = credentials
        self.client = None
        self.logger = None
        self.errors = {}

    def get_client(self) -> google.cloud.logging.client.Client:
        if self.client is None:
            self.client = logging.Client(credentials=self.service_account, project=self.service_account.project_id)
        return self.client

    def create_logger(self, log_name: str) -> google.cloud.logging.logger.Logger:
        if self.client is None:
            raise RuntimeError('Stack Driver Client is not configured')
        self.logger = self.client.logger(log_name)
        return self.logger

    def log_struct(
            self,
            data: dict,
            severity: str = 'INFO',
            labels: dict = {},
            client: google.cloud.logging.client = None
    ):
        if self.logger is None:
            raise RuntimeError('Logger Client is not configured')
        try:
            self.logger.log_struct(data, client, severity=severity, labels=labels)
            return True
        except exceptions.GoogleAPICallError as e:
            self.errors['log_struct'] = 'GoogleAPICallError: {}'.format(e)
        return False

    def debug(self, data: dict, labels: dict = None, client: google.cloud.logging.client = None):
        return self.log_struct(data, 'DEBUG', labels, client)

    def info(self, data: dict,  labels: dict = None, client: google.cloud.logging.client = None):
        print(data)
        print(labels)
        return self.log_struct(data, 'INFO', labels, client)

    def warning(self, data: dict,  labels: dict = None, client: google.cloud.logging.client = None):
        print(data)
        print(labels)
        return self.log_struct(data, 'WARNING', labels, client)

    def error(self, data: dict,  labels: dict = None, client: google.cloud.logging.client = None):
        print(data)
        print(labels)
        return self.log_struct(data, 'ERROR', labels, client)

    def critical(self, data: dict,  labels: dict = None, client: google.cloud.logging.client = None):
        print(data)
        print(labels)
        return self.log_struct(data, 'CRITICAL', labels, client)
