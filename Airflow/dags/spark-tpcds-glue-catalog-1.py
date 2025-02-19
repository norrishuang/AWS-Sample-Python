import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.operators.email import EmailOperator

'''
使用 EMR Serverless 调度依赖的例子
使用了 SparkForHiveSQL.py 执行hivesql文件。
'''


# Replace these with your correct values
APPLICATION_ID = Variable.get("SPARK_APPLICATION_ID")
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")



SPARKSQLFILE_1 = f"s3://{S3_BUCKET}/tpcds_2_4/q1-ctas.sql"
SPARKSQLFILE_2 = f"s3://{S3_BUCKET}/tpcds_2_4/q2-ctas.sql"
SPARKSQLFILE_3 = f"s3://{S3_BUCKET}/tpcds_2_4/q3-ctas.sql"
SPARKSQLFILE_4 = f"s3://{S3_BUCKET}/tpcds_2_4/q4-ctas.sql"
SPARKSQLFILE_5 = f"s3://{S3_BUCKET}/tpcds_2_4/q5-ctas.sql"

JOB_DRIVER_ARG_1 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_1}", "-s", f"{S3_BUCKET}" ,"-d", "tpcds"],
          "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.driver.cores=2 --conf spark.executor.memory=4G --conf spark.driver.memory=4G --conf spark.executor.cores=2 --conf spark.executor.instance=3"
        }
}


CONFIGURATION_OVERRIDES_ARG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {
            "logUri": f"s3://{S3_BUCKET}/sparklogs/"
        }
    }
}
# [END howto_operator_emr_serverless_config]

send_email = EmailOperator(
    task_id='send_email',
    to='12380647@qq.com',
    subject='Airflow Alert',
    html_content='<p>test email</p>',
)

with DAG(
    dag_id='emr_serverless_job_tpcds-1',
    # schedule_interval=None,
    schedule_interval='10 3 * * *',
    start_date=datetime(2023, 11, 14),
    tags=['tpcds-test'],
    catchup=False,
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # APPLICATION_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

    # [START howto_operator_emr_serverless_job]
    job_starter_1 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_1",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q1"},
        job_driver=JOB_DRIVER_ARG_1,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_1 >> send_email
# [END howto_operator_emr_serverless_job]
