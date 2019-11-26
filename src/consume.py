import os
import json
from prefect import Task, Flow
from google.oauth2 import service_account
from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.pubsub import PubSubAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter
from mysql_bigquery.prefect_utils.jobs import data
from mysql_bigquery.prefect_utils.jobs import schema
from mysql_bigquery.prefect_utils.jobs import audit


class PrefectConsumer(Task):

    def __init__(self):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.consumer_type = os.environ['MYSQL_BIG_QUERY_CONSUMER_TYPE']
        self.definitions = None
        self.service = None
        self.sd_logger = None
        self.next_queue = None

    def subscribe_to_queue(self, callback):
        pub_sub_client = PubSubAdapter(self.service_account)
        pub_sub_client.get_subscriber()
        pub_sub_client.set_subscription(f"mysql-bq-sync-{self.consumer_type}-jobs")

        pub_sub_client.subscribe(callback, max_messages=10)

    @staticmethod
    def decode_message(message) -> dict:
        data_string = bytes.decode(message.data)
        return json.loads(data_string)

    def get_service(self, message: dict):
        self.service = message['service']

    def get_definitions(self):
        self.definitions = service_helpers.get_definitions(self.service)

    def create_logger(self):
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"{self.service}-consumer-sync")

    def run_consume_job(self, message):
        if self.consumer_type == 'schema':
            schema.flow_runner(message)
        elif self.consumer_type == 'data':
            data.flow_runner(message)
        elif self.consumer_type == 'audit':
            results = audit.flow_runner(message)
            if results is False:
                message['flush'] = True
            else:
                return False
        return message

    def set_next_queue(self):
        self.next_queue = PubSubAdapter(self.service_account)
        self.next_queue.get_publisher()
        if self.consumer_type == 'schema':
            self.next_queue.set_topic(f"mysql-bq-sync-data-jobs")
        elif self.consumer_type == 'data':
            self.next_queue.set_topic(f"mysql-bq-sync-audit-jobs")
        elif self.consumer_type == 'audit':
            self.next_queue.set_topic(f"mysql-bq-sync-schema-jobs")

    def install_next_job(self, next_job_message: dict) -> bool:
        message_string = json.dumps(next_job_message)
        results = self.next_queue.publish_message(message_string)
        if results is False:
            self.sd_logger.error(
                self.next_queue.errors,
                {'class': 'PrefectConsumer', 'method': 'install_next_job'}
            )
        return results


def flow_runner(message_object):
    with Flow('Job Consumer') as flow:
        consumer = PrefectConsumer()
        decoded = consumer.decode_message(message_object)
        consumer.get_service(decoded)
        consumer.set_next_queue()
        consumer.create_logger()
        consumer.get_definitions()
        new_message = consumer.run_consume_job(decoded)
        message_object.ack()
        if new_message is not False:
            consumer.install_next_job(new_message)
    flow.run()


prefect_consumer = PrefectConsumer()
prefect_consumer.subscribe_to_queue(flow_runner)
