import pytest
import google
from mock import Mock
from google.api_core import exceptions
from google.oauth2 import service_account
from mysql_bigquery.adapters.pubsub import PubSubAdapter


@pytest.fixture(scope='function')
def mock_service_account():
    mock = Mock(spec=service_account)
    mock.project_id = 'test'
    return mock


@pytest.fixture(scope='function')
def mock_publisher(mock_service_account):
    mock_publisher = Mock(spec=google.cloud.pubsub_v1.publisher.client.Client)
    ps = PubSubAdapter(mock_service_account)
    ps.publish_client = mock_publisher
    return ps


@pytest.fixture(scope='function')
def mock_subscriber(mock_service_account):
    mock_subscriber = Mock(spec=google.cloud.pubsub_v1.subscriber.client.Client)
    ps = PubSubAdapter(mock_service_account)
    ps.subscriber_client = mock_subscriber
    return ps


@pytest.fixture(scope='function')
def mock_subscriber_configured(mock_subscriber):
    mock_subscriber.set_subscription('test')
    mock_subscriber.set_topic('test')
    mock_subscriber.get_subscriber()
    return mock_subscriber


@pytest.fixture(scope='function')
def mock_sub_future(mock_subscriber_configured):
    mock_future = Mock(spec=google.cloud.pubsub_v1.subscriber.futures.StreamingPullFuture)
    mock_subscriber_configured.subscriber_client.subscribe.return_value = mock_future
    return mock_subscriber_configured


# ~~~~~~~~~~ Set Up Tests ~~~~~~~~~~


def test_get_publisher_success(mock_service_account):
    ps = PubSubAdapter(service_account)
    result = ps.get_publisher()
    assert isinstance(result, google.cloud.pubsub_v1.PublisherClient)


def test_get_subscriber_success(mock_service_account):
    ps = PubSubAdapter(service_account)
    result = ps.get_subscriber()
    assert isinstance(result, google.cloud.pubsub_v1.SubscriberClient)


def test_set_topic_success(mock_service_account):
    ps = PubSubAdapter(mock_service_account)
    ps.set_topic('test')
    assert ps.topic == 'projects/test/topics/test'


def test_set_subscription_success(mock_service_account):
    ps = PubSubAdapter(mock_service_account)
    ps.set_subscription('test')
    assert ps.subscription == 'projects/test/subscriptions/test'

# ~~~~~~~~~~ Create Topic Tests ~~~~~~~~~~


def test_create_topic_success(mock_publisher):
    mock_publisher.publish_client.create_topic.return_value = google.cloud.pubsub_v1.types.Topic()
    mock_publisher.set_topic('test')
    result = mock_publisher.create_topic()
    assert result
    assert not mock_publisher.errors


def test_create_topic_runtime_error_no_topic(mock_publisher):
    with pytest.raises(RuntimeError):
        mock_publisher.create_topic()


def test_create_topic_wrong_return(mock_publisher):
    mock_publisher.publish_client.create_topic.return_value = False
    mock_publisher.set_topic('test')
    result = mock_publisher.create_topic()
    assert result is False
    assert mock_publisher.errors


def test_create_topic_google_api_call_error(mock_publisher):
    mock_publisher.publish_client.create_topic.side_effect = exceptions.Conflict('message')
    mock_publisher.set_topic('test')
    result = mock_publisher.create_topic()
    assert result is False
    assert mock_publisher.errors


def test_create_topic_retry_error(mock_publisher):
    mock_publisher.publish_client.create_topic.side_effect = exceptions.RetryError('message', 'cause')
    mock_publisher.set_topic('test')
    result = mock_publisher.create_topic()
    assert result is False
    assert mock_publisher.errors


def test_create_topic_value_error(mock_publisher):
    mock_publisher.publish_client.create_topic.side_effect = ValueError('message')
    mock_publisher.set_topic('test')
    result = mock_publisher.create_topic()
    assert result is False
    assert mock_publisher.errors

# ~~~~~~~~~~ Create Subscription Tests ~~~~~~~~~~


def test_create_subscription_success(mock_subscriber_configured):
    mock_subscriber_configured.subscriber_client.create_subscription.return_value = google.cloud.pubsub_v1.types.Subscription()
    result = mock_subscriber_configured.create_subscription()
    assert result
    assert not mock_subscriber_configured.errors


def test_create_subscription_runtime_error_no_topic(mock_subscriber):
    with pytest.raises(RuntimeError):
        mock_subscriber.get_subscriber()
        mock_subscriber.create_subscription()


def test_create_subscription_wrong_result(mock_subscriber_configured):
    mock_subscriber_configured.subscriber_client.create_subscription.return_value = False
    result = mock_subscriber_configured.create_subscription()
    assert result is False
    assert mock_subscriber_configured.errors


def test_create_subscription_runtime_error_no_subscription(mock_subscriber):
    with pytest.raises(RuntimeError):
        mock_subscriber.set_topic('test')
        mock_subscriber.get_subscriber()
        mock_subscriber.create_subscription()


def test_create_subscription_google_api_call_error(mock_subscriber_configured):
    mock_subscriber_configured.subscriber_client.create_subscription.side_effect = exceptions.Conflict('message')
    result = mock_subscriber_configured.create_subscription()
    assert result is False
    assert mock_subscriber_configured.errors


def test_create_subscription_retry_error(mock_subscriber_configured):
    mock_subscriber_configured.subscriber_client.create_subscription.side_effect = exceptions.RetryError('message', 'cause')
    result = mock_subscriber_configured.create_subscription()
    assert result is False
    assert mock_subscriber_configured.errors


def test_create_subscription_value_error(mock_subscriber_configured):
    mock_subscriber_configured.subscriber_client.create_subscription.side_effect = ValueError('message')
    result = mock_subscriber_configured.create_subscription()
    assert result is False
    assert mock_subscriber_configured.errors

# ~~~~~~~~~~ Publish Message Tests ~~~~~~~~~~


def test_publish_message_success(mock_publisher):
    mock_publisher.publish_client.publish.return_value = google.cloud.pubsub_v1.publisher.futures.Future()
    mock_publisher.set_topic('test')
    result = mock_publisher.publish_message('test')
    assert result
    assert not mock_publisher.errors


def test_publish_message_wrong_return(mock_publisher):
    mock_publisher.publish_client.publish.return_value = False
    mock_publisher.set_topic('test')
    result = mock_publisher.publish_message('test')
    assert result is False
    assert mock_publisher.errors


def test_publish_message_no_topic_exception(mock_publisher):
    with pytest.raises(RuntimeError):
        mock_publisher.publish_message('test')


def test_publish_message_google_api_call_error(mock_publisher):
    mock_publisher.publish_client.publish.side_effect = exceptions.Conflict('message')
    mock_publisher.set_topic('test')
    result = mock_publisher.publish_message('test')
    assert result is False
    assert mock_publisher.errors


def test_publish_message_retry_error(mock_publisher):
    mock_publisher.publish_client.publish.side_effect = exceptions.RetryError('message', 'cause')
    mock_publisher.set_topic('test')
    result = mock_publisher.publish_message('test')
    assert result is False
    assert mock_publisher.errors


def test_publish_message_value_error(mock_publisher):
    mock_publisher.publish_client.publish.side_effect = ValueError('message')
    mock_publisher.set_topic('test')
    result = mock_publisher.publish_message('test')
    assert result is False
    assert mock_publisher.errors

# ~~~~~~~~~~ Subscribe Tests ~~~~~~~~~~


def test_subscribe_success(mock_sub_future):
    mock_sub_future.get_subscriber()
    mock_sub_future.set_subscription('test')
    mock_sub_future.subscribe(callback)
    assert not mock_sub_future.errors


def test_subscribe_no_subscription_exception(mock_sub_future):
    mock_sub_future.subscription = None
    with pytest.raises(RuntimeError):
        mock_sub_future.subscribe(callback)


def test_subscribe_google_api_call_error(mock_sub_future):
    mock_sub_future.subscriber_client.subscribe.side_effect = exceptions.Conflict('message')
    mock_sub_future.set_subscription('test')
    result = mock_sub_future.subscribe(callback)
    assert result is False
    assert mock_sub_future.errors


def test_subscribe_retry_error(mock_sub_future):
    mock_sub_future.subscriber_client.subscribe.side_effect = exceptions.RetryError('message', 'cause')
    mock_sub_future.set_subscription('test')
    result = mock_sub_future.subscribe(callback)
    assert result is False
    assert mock_sub_future.errors


def test_subscribe_value_error(mock_sub_future):
    mock_sub_future.subscriber_client.subscribe.side_effect = ValueError('message')
    mock_sub_future.set_subscription('test')
    result = mock_sub_future.subscribe(callback)
    assert result is False
    assert mock_sub_future.errors


def test_subscribe_timeout_error(mock_sub_future):
    mock_sub_future.subscriber_client.subscribe.side_effect = google.cloud.pubsub_v1.publisher.exceptions.TimeoutError('message')
    mock_sub_future.set_subscription('test')
    result = mock_sub_future.subscribe(callback)
    assert result is False
    assert mock_sub_future.errors


def callback():
    return True
