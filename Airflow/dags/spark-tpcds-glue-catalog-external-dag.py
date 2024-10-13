import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.sensors.external_task import ExternalTaskSensor


'''
使用 EMR Serverless 调度依赖的例子
使用了 SparkForHiveSQL.py 执行hivesql文件。
'''


# Replace these with your correct values
APPLICATION_ID = Variable.get("SPARK_APPLICATION_ID")
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")



SPARKSQLFILE_1 = f"s3://{S3_BUCKET}/tpcds_2_4/q1.sql"
SPARKSQLFILE_2 = f"s3://{S3_BUCKET}/tpcds_2_4/q2.sql"
SPARKSQLFILE_3 = f"s3://{S3_BUCKET}/tpcds_2_4/q3.sql"
SPARKSQLFILE_4 = f"s3://{S3_BUCKET}/tpcds_2_4/q4.sql"
SPARKSQLFILE_5 = f"s3://{S3_BUCKET}/tpcds_2_4/q5.sql"

JOB_DRIVER_ARG_1 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_1}", "-s", f"{S3_BUCKET}" ,"-d", "tpcds"],
          "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.driver.cores=2 --conf spark.executor.memory=4G --conf spark.driver.memory=2G --conf spark.executor.cores=2"
        }
}

JOB_DRIVER_ARG_2 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments":["-f", f"{SPARKSQLFILE_2}", "-s", f"{S3_BUCKET}" ,"-d", "tpcds"],
          "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.driver.cores=2 --conf spark.executor.memory=4G --conf spark.driver.memory=2G --conf spark.executor.cores=2"
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



with DAG(
    dag_id='spark-tpcds-glue-catalog-external-dag',
    # schedule_interval=None,
    schedule_interval='30 10 * * *',
    start_date=datetime(2024, 8, 31),
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

    job_starter_2 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_2",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q2"},
        job_driver=JOB_DRIVER_ARG_2,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

monitor_external_dag = ExternalTaskSensor(
    task_id='external-dag-task-demo',
    external_dag_id='emr_serverless_job_tpcds',        # 需要等待的外部DAG id
    external_task_id='tpcds_q_5',        # 需要等待的外部Task id
    execution_delta=timedelta(minutes=5),    # 执行时间差，这里指定5分钟，那么当前ExternalTaskSensor会基于当前执行时间（1:05）往前倒5分钟（1:00）寻找在这个时间点依赖 task 已经成功执行完毕
    ## 假如「**init.common.1d**」的执行规则是「10 1 * * *」也就是每天凌晨1点10分，
    ## 那么这里可以使用「execution_date_fn」，让当前DAG等待至1点10分，
    ## 直到「**init.common.1d**」的「save_env_conf」成功执行完
    # execution_date_fn=lambda dt: dt + timedelta(minutes=5),
    timeout=1200,                             # 超时时间，如果等待了 1200 秒还未符合期望状态的外部Task，那么抛出异常进入重试
    allowed_states=['success'],              # Task允许的状态，这里只允许外部Task执行状态为'success'
    mode='reschedule',                       # reschedule模式，在等待的时候，两次检查期间会sleep当前Task，节约系统开销
    check_existence=True,                    # 校验外部Task是否存在，不存在立马结束等待
    dag=dag,
)


monitor_external_dag >> job_starter_1 >> job_starter_2
# [END howto_operator_emr_serverless_job]
