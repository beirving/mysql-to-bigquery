import os
import pandas as pd
from pandas import DataFrame
from google.cloud import bigquery
from cryptography.fernet import Fernet
from google.oauth2 import service_account

from mysql_bigquery.adapters.bigquery import BigQueryAdapter
from mysql_bigquery.adapters.storage import StorageAdapter


def get_definitions(service: str) -> dict:
    service_account_obj = service_account.Credentials.from_service_account_file(
        os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH']
    )

    storage = StorageAdapter(service_account_obj)
    storage.get_client()
    key = storage.get_file_as_string(bucket="mysql_sync_keys", file=service)
    if key is False:
        raise RuntimeError(storage.errors)
    crypto = Fernet(key)

    bqa = BigQueryAdapter(service_account_obj)
    bqa.get_client()
    query = f"SELECT * FROM mysql_sync.data_sources WHERE service = '{service}'"
    result = bqa.query(query)
    if result is False:
        raise RuntimeError(bqa.errors)
    definitions = {}
    for item in result:
        definitions['service'] = item['service']
        definitions['data_set'] = item['data_set']
        definitions['database'] = item['database']
        definitions['host'] = crypto.decrypt(bytes(item['host'], encoding='utf-8'))
        definitions['user'] = crypto.decrypt(bytes(item['user'], encoding='utf-8'))
        definitions['password'] = crypto.decrypt(bytes(item['password'], encoding='utf-8'))
    return definitions


def generate_bq_schema_from_mysql(mysql_schema) -> list:
    schema = []
    for column in mysql_schema:
        mysql_type = mysql_to_big_query_column(column['type'])
        if type is False:
            raise ValueError(f'Unhandled Type: {column["type"]}')
        schema.append(bigquery.SchemaField(
            column['name'],
            mysql_type,
            description=column['comment']
        ))
    return schema


def mysql_to_big_query_column(look_up: str) -> str:
    look_up_dict = {
        'TINYINT': 'INTEGER',
        'SMALLINT': 'INTEGER',
        'MEDIUMINT': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'INTEGER',
        'DECIMAL': 'FLOAT',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'FLOAT',
        'BIT': 'BOOL',
        'CHAR': 'STRING',
        'VARCHAR': 'STRING',
        'BINARY': 'BYTES',
        'VARBINARY': 'BYTES',
        'TINYTEXT': 'STRING',
        'TEXT': 'STRING',
        'MEDIUMTEXT': 'STRING',
        'LONGTEXT': 'STRING',
        'BLOB': 'STRING',
        'MEDIUMBLOB': 'STRING',
        'LONGBLOB': 'STRING',
        'ENUM': 'STRING',
        'SET': 'STRING',
        'DATE': 'TIMESTAMP',
        'TIME': 'TIMESTAMP',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
    }

    if look_up.upper() in look_up_dict:
        return look_up_dict[look_up.upper()]
    else:
        return False


def convert_schema_to_dict(schema) -> dict:
    return_val = {}
    for item in schema:
        return_val[item.name] = {"name": item.name, "field_type": item.field_type}
    return return_val


def data_frame_to_schema(df: DataFrame, schema: dict, utc: bool =False):
    for schema_item in schema.items():
        column = schema_item[1]
        if column['field_type'] == 'FLOAT':
            df[column['name']] = df[column['name']].fillna(value=0.00)
            df[column['name']] = df[column['name']].astype(float)
        elif column['field_type'] == 'INTEGER':
            df[column['name']] = df[column['name']].fillna(value=0)
            df[column['name']] = df[column['name']].astype(int)
        elif column['field_type'] == 'STRING':
            df[column['name']] = df[column['name']].astype(str)
        elif column['field_type'] == 'TIMESTAMP':
            df[column['name']] = pd.to_datetime(df[column['name']], utc=utc, errors='coerce')
        else:
            raise ValueError(f"Unhandled data type {column['field_type']} in column '{column['name']}'")
    return df


def label_mysql_table_definitions(item: dict) -> dict:
    return {
        'name': item['name'],
        'type': item['type'],
        'comment': item['comment'],
        'default': item['default'],
        'key': item['key'],
        'extra': item['extra'],
        'table': item['table'],
        'position': item['position']
    }


def guess_pk(columns: list):
    pks = []
    for column in columns:
        if column['key'] == 'PRI':
            pks.append(column['name'])
    if len(pks) > 0:
        return ','.join(pks)

    return None


def guess_watched(columns: list) -> str:
    watched = []
    candidates = []
    # Loop through all columns
    for column in columns:
        # check if a column is a date type field
        if column['type'] == 'timestamp' or column['type'] == 'datetime':
            # if Extra has CURRENT_TIMESTAMP on update this is most likely our column
            if column['extra'] == 'on update CURRENT_TIMESTAMP':
                if column['default'] == 'CURRENT_TIMESTAMP':
                    return column['name']
                else:
                    candidates.append(column['name'])
            # if the COLUMN_DEFAULT is CURRENT_TIMESTAMP
            elif column['default'] == 'CURRENT_TIMESTAMP':
                candidates.append(column['name'])

    if len(candidates) > 0:
        for candidate in candidates:
            if 'update' in candidate:
                return column['name']
            elif 'created' in candidate:
                watched.append(column['name'])

    if len(watched) > 0:
        # return None here if there are too many at this point
        # we can't determine with logic and a human needs to make a choice
        return None