import os
import pytest
import google
import pandas as pd
import concurrent
from mock import Mock, patch
from google.cloud import bigquery
from google.api_core import exceptions
from google.oauth2 import service_account
import mysql_bigquery.adapters.bigquery as big_query_adapter
from mysql_bigquery.adapters.bigquery import BigQueryAdapter


@pytest.fixture(scope='function')
def mock_service_account():
    return service_account.Credentials.from_service_account_file(os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH'])


@pytest.fixture(scope='function')
def mock_configured_client(mock_service_account):
    bq = BigQueryAdapter(mock_service_account)
    bq.get_client()
    return bq


def test_generate_params():
    input_data = [{
        'variable': 'test',
        'type': 'test',
        'value': 'test'
    }]
    output = big_query_adapter.generate_params(input_data)
    assert isinstance(output, list)
    for test_output in output:
        assert isinstance(test_output, bigquery.ScalarQueryParameter)


def test_get_client_success(mock_service_account):
    bq = BigQueryAdapter(mock_service_account)
    bq.get_client()
    assert isinstance(bq.client, google.cloud.bigquery.client.Client)


def test_check_dependencies_client(mock_service_account):
    bq = BigQueryAdapter(mock_service_account)
    with pytest.raises(RuntimeError):
        bq.check_dependencies()


def test_check_dependencies_data_set(mock_configured_client):
    with pytest.raises(RuntimeError):
        mock_configured_client.check_dependencies(True, True)


def test_check_dependencies_table(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    with pytest.raises(RuntimeError):
        mock_configured_client.check_dependencies(True, True, True)


def test_set_data_set_ref_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    assert isinstance(mock_configured_client.data_set_ref, google.cloud.bigquery.dataset.DatasetReference)


def test_set_table_ref_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    assert isinstance(mock_configured_client.table_ref, google.cloud.bigquery.table.TableReference)


def test_check_dataset_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_dataset.return_value = Mock(spec=google.cloud.bigquery.dataset.Dataset)
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_dataset('test_data_set')
        assert result
        assert not mock_configured_client.errors


def test_check_dataset_wrong_type(mock_configured_client):
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_dataset.return_value = False
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_dataset('test_data_set')
        assert result is False
        assert mock_configured_client.errors


def test_check_dataset_google_api_call_error(mock_configured_client):
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_dataset.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_dataset('test_data_set')
        assert result is False
        assert mock_configured_client.errors


def test_check_table_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_table.return_value = Mock(spec=google.cloud.bigquery.table.Table)
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_table()
        assert result
        assert not mock_configured_client.errors


def test_check_table_wrong_type(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_table.return_value = False
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_table()
        assert result is False
        assert mock_configured_client.errors


def test_check_table_google_api_call_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_table.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.check_table()
        assert result is False
        assert mock_configured_client.errors


def test_create_data_set_table_exists(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_dataset.return_value = Mock(spec=google.cloud.bigquery.dataset.Dataset)
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_data_set()
        assert result is False
        assert mock_configured_client.errors


def test_create_data_set_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_dataset.return_value = Mock(spec=google.cloud.bigquery.dataset.Dataset)
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_data_set()
        assert result
        assert not mock_configured_client.errors


def test_create_data_set_exception(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_dataset.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_data_set()
        assert result is False
        assert mock_configured_client.errors


def test_create_table_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    schema = [
        bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    ]
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_table.return_value = Mock(spec=google.cloud.bigquery.table.Table)
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_table(schema)
        assert result
        assert not mock_configured_client.errors


def test_create_table_wrong_type(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    schema = [
        bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    ]
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_table.return_value = False
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_table(schema)
        assert result is False
        assert mock_configured_client.errors


def test_create_table_api_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    schema = [
        bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    ]
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_table.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.create_table(schema)
        assert result is False
        assert mock_configured_client.errors


def test_create_table_runtime_error(mock_service_account):
    bq = BigQueryAdapter(mock_service_account)
    schema = [
        bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    ]
    with pytest.raises(RuntimeError):
        bq.create_table(schema)


def test_delete_table_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.create_table.return_value = True
        mock_configured_client.client = mock_client
        result = mock_configured_client.delete_table()
        assert result
        assert not mock_configured_client.errors


def test_delete_table_api_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.delete_table.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.delete_table()
        assert result is False
        assert mock_configured_client.errors


def test_get_schema_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_table = Mock(spec=google.cloud.bigquery.table.Table)
        mock_table.schema = [
            bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
        ]
        mock_client.get_table.return_value = mock_table
        mock_configured_client.client = mock_client
        result = mock_configured_client.get_schema()
        assert type(result) is list
        assert len(result) > 0
        assert not mock_configured_client.errors


def test_get_schema_cached_table_ref_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    mock_table = Mock(spec=google.cloud.bigquery.table.Table)
    mock_table.schema = [
        bigquery.SchemaField("column1", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column2", "INTEGER", mode="REQUIRED"),
    ]
    mock_configured_client.table = mock_table
    result = mock_configured_client.get_schema()
    assert type(result) is list
    assert len(result) > 0
    assert not mock_configured_client.errors


def test_get_schema_api_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_table.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.get_schema()
        print(mock_configured_client.errors)
        assert type(result) is list
        assert len(result) == 0
        assert mock_configured_client.errors


def test_get_schema_concurrent_futures_timeout_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('test_data_set')
    mock_configured_client.set_table_ref('test_table')
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.get_table.side_effect = concurrent.futures.TimeoutError('error')
        mock_configured_client.client = mock_client
        result = mock_configured_client.get_schema()
        assert type(result) is list
        assert len(result) == 0
        assert mock_configured_client.errors


def test_query_success(mock_configured_client):
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_query_job = Mock(spec=google.cloud.bigquery.job.QueryJob)
        mock_query_job.result.return_value = True
        mock_client.query.return_value = mock_query_job
        mock_configured_client.client = mock_client
        result = mock_configured_client.query(" ", [{'variable': 'variable', 'type': 'type', 'value': 'value'}])
        assert result
        assert not mock_configured_client.errors


def test_query_wrong_type(mock_configured_client):
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.query.returns = False
        mock_configured_client.client = mock_client
        result = mock_configured_client.query(" ")
        assert result is False
        assert mock_configured_client.errors


def test_query_api_error(mock_configured_client):
    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_client.query.side_effect = exceptions.BadRequest('bad request')
        mock_configured_client.client = mock_client
        result = mock_configured_client.query(" ")
        assert result is False
        assert mock_configured_client.errors


def test_upload_data_frame_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')
    mock_configured_client.set_table_ref('table')

    df = Mock(spec=pd.core.frame.DataFrame)
    df.to_gbq.return_value = True
    result = mock_configured_client.upload_data_frame(df)

    assert result
    assert not mock_configured_client.errors


def test_upload_data_frame_raise_exception(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')
    mock_configured_client.set_table_ref('table')

    df = Mock(spec=pd.core.frame.DataFrame)
    df.to_gbq.return_value = False
    df.to_gbq.side_effect = Exception('test')
    result = mock_configured_client.upload_data_frame(df)

    assert result is False
    assert mock_configured_client.errors


def test_copy_table_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')

    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_copy_job = Mock(spec=google.cloud.bigquery.job.CopyJob)
        mock_copy_job.done.return_value = True
        mock_client.copy_table.return_value = mock_copy_job
        mock_configured_client.client = mock_client

        mock_table = Mock(spec=google.cloud.bigquery.table.TableReference)

        result = mock_configured_client.copy_table(mock_table, mock_table)

        assert result
        assert not mock_configured_client.errors


def test_copy_table_api_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')

    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_copy_job = Mock(spec=google.cloud.bigquery.job.CopyJob)
        mock_copy_job.done.side_effect = exceptions.BadRequest('bad request')
        mock_client.copy_table.return_value = mock_copy_job
        mock_configured_client.client = mock_client

        mock_table = Mock(spec=google.cloud.bigquery.table.TableReference)

        result = mock_configured_client.copy_table(mock_table, mock_table)

        assert result is False
        assert mock_configured_client.errors


def test_export_table_to_storage_success(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')

    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_extract_job = Mock(spec=google.cloud.bigquery.job.ExtractJob)
        mock_extract_job.done.return_value = True
        mock_client.extract_table.return_value = mock_extract_job
        mock_configured_client.client = mock_client

        mock_table = Mock(spec=google.cloud.bigquery.table.TableReference)

        result = mock_configured_client.export_table_to_storage(mock_table, 'destination')

        assert result
        assert not mock_configured_client.errors


def test_export_table_to_storage_api_error(mock_configured_client):
    mock_configured_client.set_data_set_ref('dataset')

    with patch.object(google.cloud.bigquery.client, 'Client', autospec=True) as mock_client:
        mock_extract_job = Mock(spec=google.cloud.bigquery.job.ExtractJob)
        mock_extract_job.done.side_effect = exceptions.BadRequest('bad request')
        mock_client.extract_table.return_value = mock_extract_job
        mock_configured_client.client = mock_client

        mock_table = Mock(spec=google.cloud.bigquery.table.TableReference)

        result = mock_configured_client.export_table_to_storage(mock_table, 'destination')

        assert result is False
        assert mock_configured_client.errors