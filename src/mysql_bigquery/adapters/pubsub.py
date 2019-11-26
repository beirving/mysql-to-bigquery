import google
from google.cloud import pubsub
from google.oauth2 import service_account
from google.cloud.pubsub_v1.publisher import exceptions


class PubSubAdapter:

    def __init__(self, credentials: service_account):
        self.service_account = credentials
        self.topic = None
        self.subscription = None
        self.publish_client = None
        self.subscriber_client = None
        self.errors = {}

    def get_publisher(self) -> google.cloud.pubsub_v1.PublisherClient:
        if self.publish_client is None:
            self.publish_client = pubsub.PublisherClient(credentials=self.service_account)
        return self.publish_client

    def get_subscriber(self) -> google.cloud.pubsub_v1.SubscriberClient:
        if self.subscriber_client is None:
            self.subscriber_client = pubsub.SubscriberClient(credentials=self.service_account)
        return self.subscriber_client

    def set_topic(self, topic: str):
        self.topic = 'projects/{project_id}/topics/{topic}'.format(
            project_id=self.service_account.project_id,
            topic=topic
        )

    def set_subscription(self, subscription: str):
        self.subscription = 'projects/{project_id}/subscriptions/{sub}'.format(
            project_id=self.service_account.project_id,
            sub=subscription
        )

    def create_topic(self) -> bool:
        if self.topic is None:
            raise RuntimeError('Topic is not set')

        try:
            return_value = self.publish_client.create_topic(self.topic)
            if isinstance(return_value, google.cloud.pubsub_v1.types.Topic):
                return True
            else:
                self.errors['create_topic'] = 'Return was was of the type google.cloud.pubsub_v1.types.Topic'
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.errors['create_topic'] = 'Error: {}'.format(e)
        except google.api_core.exceptions.RetryError as e:
            self.errors['create_topic'] = 'Error: {}'.format(e)
        except ValueError as e:
            self.errors['create_topic'] = 'Error: {}'.format(e)
        return False

    def create_subscription(self, ack_deadline_seconds: int = 600) -> bool:
        if self.topic is None:
            raise RuntimeError('Topic is not set')

        if self.subscription is None:
            raise RuntimeError('Subscription is not set')

        try:
            result = self.subscriber_client.create_subscription(
                name=self.subscription,
                topic=self.topic,
                ack_deadline_seconds=ack_deadline_seconds
            )
            if isinstance(result, google.cloud.pubsub_v1.types.Subscription):
                return True
            else:
                self.errors['create_subscription'] = 'result type was not google.cloud.pubsub_v1.types.Subscription'
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.errors['create_subscription'] = 'GoogleAPICallError: {}'.format(e)
        except google.api_core.exceptions.RetryError as e:
            self.errors['create_subscription'] = 'RetryError: {}'.format(e)
        except ValueError as e:
            self.errors['create_subscription'] = 'ValueError: {}'.format(e)
        return False

    def publish_message(self, message: str) -> bool:
        if self.topic is None:
            raise RuntimeError('Topic is not set')

        bytes_message = bytes(message, encoding='utf-8')
        try:
            result = self.publish_client.publish(self.topic, bytes_message)
            if isinstance(result, google.cloud.pubsub_v1.publisher.futures.Future):
                return True
            else:
                self.errors['publish_message'] = 'result type was not google.cloud.pubsub_v1.publisher.futures.Future'
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.errors['publish_message'] = 'GoogleAPICallError: {}'.format(e)
        except google.api_core.exceptions.RetryError as e:
            self.errors['publish_message'] = 'RetryError: {}'.format(e)
        except ValueError as e:
            self.errors['publish_message'] = 'ValueError: {}'.format(e)
        return False

    def subscribe(self, callback, max_messages: int = 1):
        if self.subscription is None:
            raise RuntimeError('Subscription is not set')
        flow_control = pubsub.types.FlowControl(max_messages=max_messages)
        try:
            future = self.subscriber_client.subscribe(self.subscription, callback=callback, flow_control=flow_control)
            while future.running() is True:
                future.result()
            return future.done()
        except KeyboardInterrupt as e:
            self.errors['subscribe'] = 'KeyboardInterrupt: {}'.format(e)
        except exceptions.TimeoutError as e:
            self.errors['subscribe'] = 'TimeoutError: {}'.format(e)
        except google.api_core.exceptions.GoogleAPICallError as e:
            self.errors['subscribe'] = 'GoogleAPICallError: {}'.format(e)
        except google.api_core.exceptions.RetryError as e:
            self.errors['subscribe'] = 'RetryError: {}'.format(e)
        except ValueError as e:
            self.errors['subscribe'] = 'ValueError: {}'.format(e)

        return False
