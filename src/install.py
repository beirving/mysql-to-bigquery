from prefect import task, Flow
import mysql_bigquery.prefect_utils.install.storage as storage_install
import mysql_bigquery.prefect_utils.install.pub_sub as pub_sub_install
import mysql_bigquery.prefect_utils.install.big_query as big_query_install


@task
def storage():
    return storage_install.storage_flow_runner()


@task
def pub_sub():
    return pub_sub_install.pub_sub_flow_runner()


@task
def big_query():
    return big_query_install.big_query_flow_runner()


with Flow('Service Installer') as flow:
    storage()
    pub_sub()
    big_query()


state = flow.run()

