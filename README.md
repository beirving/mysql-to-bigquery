#Mysql to BigQuery
Scripts to synchronize a MySQL data base to a BigQuery data set 

---
#Note on the State of the Code Base

Currently I am not working on this codebase.

I am aware the the docker part of this codebase is not working but with the proper environmental variables it does function.

---

#Installation
* git clone https://gitlab.madwire.io/pipeline/mysql-to-bigquery.git
* cd to clone location
* pip install .
* Create credentials/google_auth.json a Service Account JSO 
   * https://cloud.google.com/video-intelligence/docs/common/auth 
   * Required following roles:
     * BigQuery Admin
     * Stackdriver Debugger User
     * Cloud Trace Admin
     * Logging Admin
     * Pub/Sub Admin
     * Storage Admin

* Export Environmental Variables
  * MYSQL_BIG_QUERY_GOOGLE_AUTH: "/credentials/google_auth.json"
---
# Usage
## install.py
Running this script will do the following:
* Creates GCP storage folder "mysql_sync_keys"
* Create the following PubSub topics and subscriptions
  * mysql-bq-sync-schema-jobs
  * mysql-bq-sync-data-jobs
  * mysql-bq-sync-audit-jobs
* Create BigQuery table "data_sources.mysql_sync"

## add_service.py
Running this script will walk you through installing a service to be synchronized
### CLI Prompts
* Service Name: the name of the database to be synchronized, this is just a reference name not the host path
* Data Set: name of the data set as you want it to appear in BigQuery
* Host: database connection string
* Username: database username
* Password: password for the username
* Database: Database to synchronize
### Behind the scenes
* Stores encrypted credentials in BigQuery "data_sources.mysql_sync" table
* Generates encryption key and stores that in storage "mysql_sync_keys"

---

## produce.py
Running this script on an interval will produce installed services to the "mysql-bq-sync-schema-jobs" Pubsub topic

Producing a service kicks off the synchronization process
---
## consume.py
Consumers will pull from one of three Pubsub subscriptions based on the configuration of the consumer

## PubSub Topics and Consumer Message Flow
Consumers pull of the topics and will do the work as well as move the message to the new topic based on the success of the job.

###Message Flow
Messages move from topic to topic according to the following logic
* Produced from produce.py ->  mysql-bq-sync-schema-jobs
* Messages consumed from mysql-bq-sync-schema-jobs -> mysql-bq-sync-data-jobs
* Messages consumed from mysql-bq-sync-data-jobs -> mysql-bq-sync-audit-jobs
* Messages consumed from mysql-bq-sync-audit-jobs and fail their audit -> mysql-bq-sync-schema-jobs