import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator


# Replace these with your correct values
APPLICATION_ID = Variable.get("APPLICATION_ID")
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")
DBUSER = Variable.get("DBUSER")
DBPASSWORD = Variable.get("DBPASSWORD")
MYSQLHOST= Variable.get("MYSQLHOST")
HOUR=23
DT="2016-01-01"
NUM=1
JDBCDriverClass="org.mariadb.jdbc.Driver"
JDBCDriver="mysql-connector-java.jar"

# [START howto_operator_emr_serverless_config]
JOB_DRIVER_ARG = {
    "hive": {
            "query": f"s3://{S3_BUCKET}/script/emr-serverless-hive-trip-01.sql",
            "parameters": f"--hivevar NUM={NUM} --hivevar HOUR={HOUR} --hivevar DT={DT} --hiveconf hive.exec.scratchdir=s3://{S3_BUCKET}/hive/scratch --hiveconf hive.metastore.warehouse.dir=s3://{S3_BUCKET}/hive/warehouse"
        }
}

CONFIGURATION_OVERRIDES_ARG = {
     "applicationConfiguration": [
            {
                "classification": "hive-site",
                    "properties": {
                        "hive.driver.cores": "2",
                        "hive.driver.memory": "4g",
                        "hive.tez.container.size": "8192",
                        "hive.tez.cpu.vcores": "4",
                        "hive.metastore.client.factory.class": "org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClientFactory",
                        "hive.blobstore.use.output-committer": "true",
                        "javax.jdo.option.ConnectionDriverName": f"{JDBCDriverClass}",
                        "javax.jdo.option.ConnectionURL": f"jdbc:mysql://{MYSQLHOST}:3306/hive",
                        "javax.jdo.option.ConnectionUserName": f"{DBUSER}",
                        "javax.jdo.option.ConnectionPassword": f"{DBPASSWORD}"
                    }
            }
        ],
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": f"s3://{S3_BUCKET}/hive-logs/"
            }
        }
}
# [END howto_operator_emr_serverless_config]

with DAG(
    dag_id='example_emr_serverless_job_hive_1',
    schedule_interval=None,
    start_date=datetime(2023, 5, 10),
    tags=['example'],
    catchup=False,
) as dag:

  # An example of how to get the cluster id and arn from an Airflow connection
  # APPLICATION_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
  # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

  # [START howto_operator_emr_serverless_job]
  job_starter = EmrServerlessStartJobOperator(
      task_id="start_job_1",
      application_id=APPLICATION_ID,
      execution_role_arn=JOB_ROLE_ARN,
      job_driver=JOB_DRIVER_ARG,
      configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
      config={"name": "Hive-NyTaxi-CLI-From-Airflow_1"},
      retries=5,
      retry_delay=timedelta(minutes=1),
  )

  job_starter
# [END howto_operator_emr_serverless_job]
