"""
Airflow DAG to create an EMR cluster, deploy Spark, and run a Spark job
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable


S3_BUCKET = Variable.get("S3_BUCKET")
SPARKSQLFILE_1 = f"s3://{S3_BUCKET}/tpcds_2_4/q1-ctas.sql"


# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# EMR集群配置
JOB_FLOW_OVERRIDES = {
    'Name': 'Spark-EMR-Cluster',
    'ReleaseLabel': 'emr-7.5.0',  # 使用EMR 7.5.0版本
    'Applications': [
        {'Name': 'Spark'},
        {'Name': 'Hadoop'},
        {'Name': 'Hive'},
        {'Name': 'Livy'}
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm7g.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core nodes',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm7g.xlarge',
                'InstanceCount': 2,
            }
        ],
        'Ec2KeyName': 'ec2-us-east-1',  # 指定EC2密钥对
        'Ec2SubnetId': 'subnet-03e89c919916ca66b',  # 指定子网ID
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'LogUri': 's3://aws-logs-812046859005-us-east-1/elasticmapreduce/',  # EMR日志路径
    'VisibleToAllUsers': True,
    'EbsRootVolumeSize': 32,
    'Configurations': [
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        },
        {
            'Classification': 'hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        }
    ]
}

# Spark作业步骤
SPARK_STEP = [
    {
        'Name': 'Spark应用程序',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--class', 'org.example.SparkApp',  # 替换为你的主类
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--executor-memory', '2g',
                '--executor-cores', '2',
                '--conf', 'spark.sql.catalogImplementation=hive',
                '--conf', 'spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                f's3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py',  # 替换为你的Spark作业JAR包路径
                "-f", f"{SPARKSQLFILE_1}", "-s", f"{S3_BUCKET}" ,"-d", "tpcds"  # 替换为你的应用程序参数
            ]
        }
    }
]

# 创建DAG
dag = DAG(
    'emr_spark_job',
    default_args=default_args,
    description='创建EMR集群并运行Spark作业',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['emr', 'spark'],
    catchup=False,
)

# 创建EMR集群
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    dag=dag,
)

# 添加Spark作业步骤
add_steps = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    steps=SPARK_STEP,
    aws_conn_id='aws_default',
    dag=dag,
)

# 监控Spark作业步骤
watch_step = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# 终止EMR集群
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_emr_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# 设置任务依赖关系
create_emr_cluster >> add_steps >> watch_step >> terminate_emr_cluster
