import google
import pytest
from os import path
from mock import Mock, patch
from google.api_core import exceptions
from google.oauth2 import service_account
from mysql_bigquery.adapters.storage import StorageAdapter


@pytest.fixture(scope='function')
def mock_service_account():
    mock = Mock(spec=service_account)
    mock.project_id = 'test'
    return mock


@pytest.fixture(scope='function')
def mock_adapter(mock_service_account):
    adapter = StorageAdapter(mock_service_account)
    mock = Mock(spec=google.cloud.storage.Client)
    adapter.client = mock
    return adapter


@pytest.fixture()
def mock_file():
    f = open("mock_file.txt", "w+")
    for i in range(10):
        f.write("This is line %d\r\n" % (i + 1))
    f.close()
    return 'mock_file.txt'


@pytest.fixture(scope='function')
def mock_bucket():
    mock_bucket = Mock(spec=google.cloud.storage.bucket.Bucket)
    return mock_bucket


def test_create_client_success(mock_adapter):
    with patch.object(google.cloud.client, 'Client', autospec=True):
        return_value = mock_adapter.get_client()
        assert isinstance(return_value, google.cloud.storage.client.Client)


def test_create_client_failure_bad_credentials(mock_service_account):
    client = StorageAdapter(mock_service_account)
    with pytest.raises(ValueError):
        client.get_client()


def test_create_bucket_success(mock_adapter):
    mock_bucket = Mock(spec=google.cloud.storage.bucket.Bucket)
    mock_bucket.exists.return_value = True
    mock_adapter.client.create_bucket.return_value = mock_bucket
    result = mock_adapter.make_bucket('string')
    assert result
    assert not mock_adapter.errors


def test_create_bucket_client_is_none(mock_service_account):
    client = StorageAdapter(mock_service_account)
    with pytest.raises(RuntimeError):
        client.make_bucket('string')


def test_create_bucket_failure(mock_adapter):
    mock_adapter.client.create_bucket.side_effect = exceptions.Conflict('string')
    result = mock_adapter.make_bucket('string')
    assert result is False
    assert mock_adapter.errors


def test_write_object_success(mock_file, mock_adapter, mock_bucket):
    mock_adapter.client.get_bucket.return_value = mock_bucket
    result = mock_adapter.write_file('bucket', mock_file, mock_file, True)
    assert result
    assert not mock_adapter.errors


def test_write_object_failure_bucket_not_exist(mock_file, mock_adapter, mock_bucket):
    mock_blob = Mock(spec=google.cloud.storage.blob.Blob)
    mock_blob.upload_from_filename.side_effect = exceptions.Conflict('string')
    mock_bucket.blob.return_value = mock_blob
    mock_adapter.client.get_bucket.return_value = mock_bucket
    result = mock_adapter.write_file('bucket', mock_file, mock_file, True)
    assert result is False
    assert mock_adapter.errors


def test_write_object_failure_no_file(mock_adapter, mock_bucket):
    mock_blob = Mock(spec=google.cloud.storage.blob.Blob)
    mock_blob.upload_from_filename.side_effect = FileNotFoundError('No File')
    mock_bucket.blob.return_value = mock_blob
    mock_adapter.client.get_bucket.return_value = mock_bucket
    result = mock_adapter.write_file('bucket', 'no-file', 'no-file', True)
    assert result is False
    assert mock_adapter.errors


def test_write_object_client_is_none(mock_service_account):
    client = StorageAdapter(mock_service_account)
    with pytest.raises(RuntimeError):
        client.write_file('bucket', 'no-file', 'no-file', True)


def test_write_object_delete_file_after(mock_adapter, mock_file, mock_bucket):
    assert path.exists(mock_file)
    mock_adapter.client.get_bucket.return_value = mock_bucket
    mock_adapter.write_file('bucket', mock_file, mock_file, True)
    assert not path.exists(mock_file)


def test_write_string_success(mock_adapter, mock_bucket):
    mock_adapter.client.get_bucket.return_value = mock_bucket
    result = mock_adapter.write_string('bucket', 'destination', 'string to write')
    assert result
    assert not mock_adapter.errors


def test_write_string_client_is_none(mock_service_account):
    client = StorageAdapter(mock_service_account)
    with pytest.raises(RuntimeError):
        client.write_string('bucket', 'destination', 'string to write')


def test_write_string_api_error(mock_adapter, mock_bucket):
    mock_blob = Mock(spec=google.cloud.storage.blob.Blob)
    mock_blob.upload_from_string.side_effect = exceptions.Conflict('string')
    mock_bucket.blob.return_value = mock_blob
    mock_adapter.client.get_bucket.return_value = mock_bucket
    result = mock_adapter.write_string('bucket', 'destination', 'string to write')
    assert result is False
    assert mock_adapter.errors
