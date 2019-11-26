import os
import json
import google
from prefect import Task, Flow
from google.oauth2 import service_account

from mysql_bigquery import service_helpers
from mysql_bigquery.adapters.pubsub import PubSubAdapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectProduceSchemaJobs(Task):
    def __init__(self, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.definitions = None
        self.sd_logger = None
        self.service = None
        super().__init__(**kwargs)

    def get_definitions(self, service_name: str):
        self.definitions = service_helpers.get_definitions(service_name)

    def create_logger(self):
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"{self.definitions['service']}-producer-sync")

    def create_pub_sub_client(self) -> PubSubAdapter:
        pub_sub_client = PubSubAdapter(self.service_account)
        pub_sub_client.get_publisher()
        pub_sub_client.set_topic("mysql-bq-sync-schema-jobs")
        return pub_sub_client

    def create_big_query_client(self) -> BigQueryAdapter:
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        big_query_client.set_data_set_ref(self.definitions['data_set'])
        big_query_client.set_table_ref('sync_tracking_table')
        return big_query_client

    def get_service_to_produce(self) -> str:
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        query ='SELECT * from mysql_sync.data_sources order by `last_run` limit 1'
        result = big_query_client.query(query)

        if not result:
            self.sd_logger.error(
                big_query_client.errors,
                {'class': 'PrefectProduceSchemaJobs', 'method': 'get_service_to_produce'}
            )
            RuntimeError(big_query_client.errors)

        for item in result:
            if hasattr(item, 'service'):
                return item.service
            else:
                raise RuntimeError('Service was not found in mysql_sync.data_sources')

    def get_items_to_be_queued(self, big_query_client: BigQueryAdapter):
        query = [
            "SELECT `table`, `watched`, `primary_id`",
            f"FROM `{self.definitions['data_set']}`.`sync_tracking_table`",
            "WHERE `synchronize` is true",
            "ORDER BY `table` ASC"
        ]

        result = big_query_client.query(' '.join(query))
        if not result:
            self.sd_logger.error(
                big_query_client.errors,
                {'class': 'PrefectProduceSchemaJobs', 'method': 'get_items_to_be_queued'}
            )
        return result

    def send_items_to_queue(
            self,
            service_name: str,
            items: google.cloud.bigquery.table.RowIterator,
            pub_sub: PubSubAdapter
    ):
        success = 0
        for item in items:
            message = {
                'service': service_name,
                'table': item['table'],
                'watched': item['watched'],
                'primary_id': item['primary_id'],
                "flush": False
            }
            message_string = json.dumps(message)
            result = pub_sub.publish_message(message_string)
            if not result:
                self.sd_logger.error(
                    pub_sub.errors,
                    {'class': 'PrefectProduceSchemaJobs', 'method': 'send_items_to_queue'}
                )
            else:
                success += 1
        self.sd_logger.info(
            {'message': f'{success} Schema Jobs Queued'},
            {'class': 'PrefectProduceSchemaJobs', 'method': 'send_items_to_queue'}
        )
        return success

    def set_last_run(self, service_name: str):
        big_query_client = BigQueryAdapter(self.service_account)
        big_query_client.get_client()
        query = f"UPDATE mysql_sync.data_sources SET `last_run` = CURRENT_TIMESTAMP where `service`= '{service_name}'"
        result = big_query_client.query(query)

        if not result:
            self.sd_logger.error(
                big_query_client.errors,
                {'class': 'PrefectProduceSchemaJobs', 'method': 'set_last_run'}
            )
        return result


with Flow('Produce Service Jobs') as flow:
    producer = PrefectProduceSchemaJobs()
    service = producer.get_service_to_produce()
    producer.get_definitions(service)
    create_big_query_client = producer.create_big_query_client()
    producer.create_logger()
    create_pub_sub_client = producer.create_pub_sub_client()
    get_items_to_be_queued = producer.get_items_to_be_queued(create_big_query_client)
    if get_items_to_be_queued:
        send_items_to_queue = producer.send_items_to_queue(
            service,
            get_items_to_be_queued,
            create_pub_sub_client
        )
        producer.set_last_run(service)

state = flow.run()
