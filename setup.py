from setuptools import setup

setup(
    name='mysql-to-bigquery',
    version='1',
    packages=['mysql_bigquery', 'mysql_bigquery.adapters', 'mysql_bigquery.prefect_utils',
              'mysql_bigquery.prefect_utils.jobs', 'mysql_bigquery.prefect_utils.install'],
    package_dir={'': '/src'},
    url='https://gitlab.madwire.io/pipeline/mysql-to-bigquery',
    license='',
    author='bradleyirving',
    author_email='bradley.irving@madwire.com',
    description=''
)
