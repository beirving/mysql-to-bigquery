import os

if os.environ.get('MYSQL_BIG_QUERY_DEFINITIONS') is None:
    os.environ['MYSQL_BIG_QUERY_DEFINITIONS'] = '/credentials/definitions.json'

if os.environ.get('MYSQL_BIG_QUERY_GOOGLE_AUTH') is None:
    os.environ['MYSQL_BIG_QUERY_GOOGLE_AUTH'] = '/credentials/google_auth.json'

if os.environ.get('MYSQL_BIG_QUERY_MYSQL_CONFIG') is None:
    os.environ['MYSQL_BIG_QUERY_MYSQL_CONFIG'] = '/credentials/config.ini'