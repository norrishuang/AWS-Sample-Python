import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.sensors.external_task import ExternalTaskSensor


# Replace these with your correct values
APPLICATION_ID = Variable.get("APPLICATION_ID")
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")
DBUSER = Variable.get("DBUSER")
DBPASSWORD = Variable.get("DBPASSWORD")
MYSQLHOST= Variable.get("MYSQLHOST")
HOUR=23
DT="2016-01-01"
NUM=4
JDBCDriverClass="org.mariadb.jdbc.Driver"
JDBCDriver="mysql-connector-java.jar"

# [START howto_operator_emr_serverless_config]
JOB_DRIVER_ARG_01 = {
    "hive": {
            "query": f"s3://{S3_BUCKET}/script/emr-serverless-hive-trip-01.sql",
            "parameters": f"--hivevar NUM={NUM} --hivevar HOUR={HOUR} --hivevar DT={DT} --hiveconf hive.exec.scratchdir=s3://{S3_BUCKET}/hive/scratch --hiveconf hive.metastore.warehouse.dir=s3://{S3_BUCKET}/hive/warehouse"
        }
}

JOB_DRIVER_ARG_02 = {
    "hive": {
        "query": f"s3://{S3_BUCKET}/script/emr-serverless-hive-trip-01.sql",
        "parameters": f"--hivevar NUM={NUM} --hivevar HOUR={HOUR} --hivevar DT={DT} --hiveconf hive.exec.scratchdir=s3://{S3_BUCKET}/hive/scratch --hiveconf hive.metastore.warehouse.dir=s3://{S3_BUCKET}/hive/warehouse"
    }
}

JOB_DRIVER_ARG_03 = {
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
    dag_id='example_emr_serverless_job_hive_6',
    schedule_interval='15 * * * *',
    start_date=datetime(2024, 4, 10),
    tags=['example'],
    catchup=False,
) as dag:

  # An example of how to get the cluster id and arn from an Airflow connection
  # APPLICATION_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
  # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

  # [START howto_operator_emr_serverless_job]
  job_starter_01 = EmrServerlessStartJobOperator(
      task_id="job_starter_01",
      application_id=APPLICATION_ID,
      execution_role_arn=JOB_ROLE_ARN,
      job_driver=JOB_DRIVER_ARG_01,
      configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
      config={"name": "Hive-NyTaxi-CLI-From-Airflow-4"},
      retries=5,
      retry_delay=timedelta(minutes=1),
  )

  job_starter_02 = EmrServerlessStartJobOperator(
      task_id="job_starter_02",
      application_id=APPLICATION_ID,
      execution_role_arn=JOB_ROLE_ARN,
      job_driver=JOB_DRIVER_ARG_02,
      configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
      config={"name": "Hive-NyTaxi-CLI-From-Airflow-4"},
      retries=5,
      retry_delay=timedelta(minutes=1),
  )

  job_starter_03 = EmrServerlessStartJobOperator(
      task_id="job_starter_03",
      application_id=APPLICATION_ID,
      execution_role_arn=JOB_ROLE_ARN,
      job_driver=JOB_DRIVER_ARG_03,
      configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
      config={"name": "Hive-NyTaxi-CLI-From-Airflow-4"},
      retries=5,
      retry_delay=timedelta(minutes=1),
  )

  # 这里实例化一个ExterTaskSensor
  monitor_common_dag_save_env_conf = ExternalTaskSensor(
      task_id='monitor_external_dag_job_5_task02',
      external_dag_id='example_emr_serverless_job_hive_5',        # 需要等待的外部DAG id
      external_task_id='job_starter_02',        # 需要等待的外部Task id
      execution_delta=timedelta(minutes=20),    # 执行时间差，这里指定5分钟，那么当前ExternalTaskSensor会基于当前执行时间（1:05）往前倒5分钟（1:00）寻找在这个时间点已经成功执行完毕的**init.common.1d**的save_env_conf
      ## 假如「**init.common.1d**」的执行规则是「10 1 * * *」也就是每天凌晨1点10分，
      ## 那么这里可以使用「execution_date_fn」，让当前DAG等待至1点10分，
      ## 直到「**init.common.1d**」的「save_env_conf」成功执行完
      # execution_date_fn=lambda dt: dt + timedelta(minutes=5),
      timeout=600,                             # 超时时间，如果等待了600秒还未符合期望状态的外部Task，那么抛出异常进入重试
      allowed_states=['success'],              # Task允许的状态，这里只允许外部Task执行状态为'success'
      mode='reschedule',                       # reschedule模式，在等待的时候，两次检查期间会sleep当前Task，节约系统开销
      check_existence=True,                    # 校验外部Task是否存在，不存在立马结束等待
      dag=dag,
 )

  job_starter_01 >> monitor_common_dag_save_env_conf >> job_starter_02 >> job_starter_03

# [END howto_operator_emr_serverless_job]
