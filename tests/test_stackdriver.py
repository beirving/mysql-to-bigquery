import google
import pytest
from mock import Mock, patch
from google.api_core import exceptions
from google.oauth2 import service_account
from mysql_bigquery.adapters.stackdriver import StackDriverAdapter


@pytest.fixture(scope='function')
def mock_service_account():
    mock = Mock(spec=service_account)
    mock.project_id = 'test'
    return mock


@pytest.fixture(scope='function')
def mock_service(mock_service_account):
    return StackDriverAdapter(mock_service_account)


@pytest.fixture(scope='function')
def mock_client(mock_service):
    with patch.object(google.cloud.logging.client, 'Client', autospec=True) as mock_client:
        mock_service.client = mock_client
    return mock_service


@pytest.fixture(scope='function')
def mock_logger(mock_service):
    with patch.object(google.cloud.logging.logger, 'Logger', autospec=True) as mock_logger:
        mock_service.logger = mock_logger
    return mock_service


def test_create_client_success(mock_service):
    with patch.object(google.cloud.logging, 'Client', autospec=True):
        mock_service.get_client()
        assert mock_service.client is not None


def test_create_logger_success(mock_client):
    with patch.object(google.cloud.client, 'Client', autospec=True):
        mock_client.create_logger('test')
        assert mock_client.logger is not None


def test_create_logger_runtime_error(mock_service):
    with pytest.raises(RuntimeError):
        mock_service.create_logger('failure')


def test_log_struct_runtime_error(mock_service):
    with pytest.raises(RuntimeError):
        mock_service.log_struct({})


def test_log_struct_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.log_struct({"test": "test"})
    assert return_value
    assert not mock_logger.errors


def test_log_struct_google_api_call_error_exception(mock_logger):
    mock_logger.logger.log_struct.side_effect = exceptions.Conflict('string')
    return_value = mock_logger.log_struct({"test": "test"})
    assert return_value is False
    assert mock_logger.errors


def test_log_debug_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.debug({"test": "test"})
    assert return_value
    assert not mock_logger.errors


def test_log_info_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.info({"test": "test"})
    assert return_value
    assert not mock_logger.errors


def test_log_warning_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.warning({"test": "test"})
    assert return_value
    assert not mock_logger.errors


def test_log_error_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.error({"test": "test"})
    assert return_value
    assert not mock_logger.errors


def test_log_critical_success(mock_logger):
    mock_logger.return_value = True
    return_value = mock_logger.critical({"test": "test"})
    assert return_value
    assert not mock_logger.errors
