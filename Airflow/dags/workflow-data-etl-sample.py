import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# glue -> emr-serverless -> glue

# Replace these with your correct values
APPLICATION_ID = "00fm3j1vh4gsnk09"
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")

GLUE_JOB_RDS_TO_S3_NAME = "workflow-rds-to-s3-1"
GLUE_JOB_S3_TO_MYSQL_NAME = "workflow-s3-to-mysql"


# [START howto_operator_emr_serverless_config]
JOB_DRIVER_ARG = {
    "hive": {
        "query": f"s3://{S3_BUCKET}/script/q1-ctas.sql",
        "parameters": "--hiveconf DT=20240802"
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
                "hive.exec.scratchdir": f"s3://{S3_BUCKET}/emr-serverless-hive/hive/scratch",
                "hive.metastore.warehouse.dir": f"s3://{S3_BUCKET}/emr-serverless-hive/hive/warehouse",
                "hive.blobstore.use.output-committer": "true"
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
        dag_id='workflow-sample-01',
        schedule_interval=None,
        start_date=datetime(2024, 9, 5),
        tags=['workflow'],
        catchup=False,
) as dag:

    # [START howto_operator_emr_serverless_job]
    emrserverless_job_starter = EmrServerlessStartJobOperator(
        task_id="start_job",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        config={"name": "hive-sql-sample"}
    )

    # [START howto_operator_glue]
    submit_glue_job_1 = GlueJobOperator(
        task_id="workflow-glue-rds-to-s3",
        job_name=GLUE_JOB_RDS_TO_S3_NAME
    )

    # [START howto_operator_glue]
    submit_glue_job_2 = GlueJobOperator(
        task_id="workflow-glue-s3-to-glue",
        job_name=GLUE_JOB_S3_TO_MYSQL_NAME
    )

submit_glue_job_1 >> emrserverless_job_starter >> submit_glue_job_2