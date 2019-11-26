import os
from prefect import Task, Flow
from google.oauth2 import service_account
from mysql_bigquery.adapters.pubsub import PubSubAdapter
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


class PrefectInstallPubSub(Task):
    def __init__(self, **kwargs):
        self.service_account = service_account.Credentials.from_service_account_file(
            os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
        )
        self.sd_logger = StackDriverAdapter(self.service_account)
        self.sd_logger.get_client()
        self.sd_logger.create_logger(f"install-pub-sub")
        super().__init__(**kwargs)

    def create_client(self):
        pub_sub_client = PubSubAdapter(self.service_account)
        pub_sub_client.get_publisher()
        return pub_sub_client

    def create_topic(self, pub_sub_client: PubSubAdapter, topic: str) -> bool:
        pub_sub_client.set_topic(
            topic
        )
        result = pub_sub_client.create_topic()
        if result:
            self.sd_logger.info(
                {'message': f"Created {topic} Topic"},
                {'class': 'PrefectInstallPubSub', 'method': 'create_topic'}
            )
        else:
            self.sd_logger.warning(
                pub_sub_client.errors,
                {'class': 'PrefectInstallPubSub', 'method': 'create_topic'}
            )
        return result

    def create_subscription(self, pub_sub_client: PubSubAdapter, topic: str, subscription: str) -> bool:
        pub_sub_client.set_topic(
            topic
        )
        pub_sub_client.set_subscription(
            subscription
        )
        pub_sub_client.get_subscriber()
        result = pub_sub_client.create_subscription()
        if result:
            self.sd_logger.info(
                {'message': f"Created {topic} Subscription"},
                {'class': 'PrefectInstallPubSub', 'method': 'create_subscription'}
            )
        else:
            self.sd_logger.warning(
                pub_sub_client.errors,
                {'class': 'PrefectInstallPubSub', 'method': 'create_subscription'}
            )
        return result


def pub_sub_flow_runner():
    with Flow('Add Pub/Sub') as flow:
        installer = PrefectInstallPubSub()
        client = installer.create_client()
        installer.create_topic(
            pub_sub_client=client,
            topic="mysql-bq-sync-schema-jobs"
        )
        installer.create_subscription(
            pub_sub_client=client,
            topic="mysql-bq-sync-schema-jobs",
            subscription="mysql-bq-sync-schema-jobs"
        )
        installer.create_topic(
            pub_sub_client=client,
            topic="mysql-bq-sync-data-jobs"
        )
        installer.create_subscription(
            pub_sub_client=client,
            topic="mysql-bq-sync-data-jobs",
            subscription="mysql-bq-sync-data-jobs"
        )
        installer.create_topic(
            pub_sub_client=client,
            topic="mysql-bq-sync-audit-jobs"
        )
        installer.create_subscription(
            pub_sub_client=client,
            topic="mysql-bq-sync-audit-jobs",
            subscription="mysql-bq-sync-audit-jobs"
        )

    return flow.run()
