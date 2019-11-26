import os
from mysql_bigquery import service_helpers

from mysql_bigquery.adapters import bigquery
from mysql_bigquery.adapters import mysql
from mysql_bigquery.adapters import pubsub
from mysql_bigquery.adapters import stackdriver
from mysql_bigquery.adapters import storage

if os.environ.get('MYSQL_BIG_QUERY_DEFINITIONS') is None:
    os.environ['MYSQL_BIG_QUERY_DEFINITIONS'] = '/credentials/definitions.json'

if os.environ.get('MYSQL_BIG_QUERY_GOOGLE_AUTH') is None:
    os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH'] = '/credentials/google_auth.json'

if os.environ.get('MYSQL_BIG_QUERY_MYSQL_CONFIG') is None:
    os.environ['MYSQL_BIG_QUERY_MYSQL_CONFIG'] = '/credentials/config.ini'