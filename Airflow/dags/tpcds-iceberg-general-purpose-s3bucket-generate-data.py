import os
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

'''
2025-01-04
生成 TPCDS 3TB 的数据，写入 S3 Bucket Iceberg
'''


# Replace these with your correct values
APPLICATION_ID = Variable.get("SPARK_APPLICATION_ID")
JOB_ROLE_ARN = Variable.get("JOB_ROLE_ARN")
S3_BUCKET = Variable.get("S3_BUCKET")

CATALOG="glue_catalog"
DATABASE="tpcds_iceberg_small_files_hash"
ENABLE_HASH="true"
FILESIZE="1048576"


SPARKSQLFILE_1 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-1.sql"
SPARKSQLFILE_2 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-2.sql"
SPARKSQLFILE_3 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-3-catalog_returns.sql"
SPARKSQLFILE_4 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-4-catalog_sales.sql"
SPARKSQLFILE_5 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-5.sql"
SPARKSQLFILE_6 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-6-store_returns.sql"
SPARKSQLFILE_7 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-7-store_sales.sql"
SPARKSQLFILE_8 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-8.sql"
SPARKSQLFILE_9 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-9-web_returns.sql"
SPARKSQLFILE_10 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-10-web_sales.sql"
SPARKSQLFILE_11 = f"s3://{S3_BUCKET}/pyspark/generate-tpcds-data-11-web_site.sql"

EXECUTOR_CORE = "2"
EXECUTOR_MEMORY = "4G"

SPARK_SUBMIT_PARAMETERS="--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.warehouse=s3://emr-hive-us-east-1-812046859005/tpcds_data_folder/iceberg-folder/ --conf spark.executor.cores=4 --conf spark.executor.memory=16g --conf spark.driver.cores=4 --conf spark.driver.memory=16g --conf spark.driver.maxResultSize=4g --conf spark.emr-serverless.driver.disk=50G --conf spark.emr-serverless.executor.disk=50G --conf spark.network.retry.count=5 --conf spark.network.retry.wait=5s --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.initialExecutors=1 --conf spark.dynamicAllocation.maxExecutors=80 --conf spark.dynamicAllocation.minExecutors=1"


JOB_DRIVER_ARG_1 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_1}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
          "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
        }
}

JOB_DRIVER_ARG_2 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_2}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
          "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
        }
}

JOB_DRIVER_ARG_3 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_3}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
          "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
        }
}

JOB_DRIVER_ARG_4 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_4}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
          "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
        }
}

JOB_DRIVER_ARG_5 = {
    "sparkSubmit": {
          "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
          "entryPointArguments": ["-f", f"{SPARKSQLFILE_5}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
          "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
        }
}

JOB_DRIVER_ARG_6 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_6}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
    }
}

JOB_DRIVER_ARG_7 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_7}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
    }
}

JOB_DRIVER_ARG_8 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_8}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
    }
}

JOB_DRIVER_ARG_9 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_9}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
    }
}

JOB_DRIVER_ARG_10 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_10}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
    }
}

JOB_DRIVER_ARG_11 = {
    "sparkSubmit": {
        "entryPoint": f"s3://{S3_BUCKET}/pyspark/SparkForHiveSQL.py",
        "entryPointArguments": ["-f", f"{SPARKSQLFILE_11}", "-s", f"{S3_BUCKET}","--hivevar", f"CATALOG=\"{CATALOG}\"", "--hivevar", f"DATABASE=\"{DATABASE}\"","--hivevar", f"ENABLE_HASH=\"{ENABLE_HASH}\"", "--hivevar", f"FILESIZE=\"{FILESIZE}\""],
        "sparkSubmitParameters": f"{SPARK_SUBMIT_PARAMETERS}"
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
    dag_id='iceberg-general-purpose-s3bucket-generate-data',
    schedule_interval=None,
    # schedule_interval='10 10 * * *',
    start_date=datetime(2024, 12, 1),
    tags=['tpcds-s3bucket'],
    catchup=False,
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # APPLICATION_ID = {{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}
    # JOB_ROLE_ARN = {{ conn.emr_eks.extra_dejson["job_role_arn"] }}

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

    job_starter_3 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_3",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q3"},
        job_driver=JOB_DRIVER_ARG_3,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_4 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_4",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q4"},
        job_driver=JOB_DRIVER_ARG_4,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_5 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_5",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q5"},
        job_driver=JOB_DRIVER_ARG_5,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_6 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_6",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q6"},
        job_driver=JOB_DRIVER_ARG_6,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_7 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_7",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q7"},
        job_driver=JOB_DRIVER_ARG_7,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_8 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_8",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q8"},
        job_driver=JOB_DRIVER_ARG_8,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_9 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_9",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q9"},
        job_driver=JOB_DRIVER_ARG_9,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_10 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_10",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q10"},
        job_driver=JOB_DRIVER_ARG_10,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_11 = EmrServerlessStartJobOperator(
        task_id="tpcds_q_11",
        application_id=APPLICATION_ID,
        execution_role_arn=JOB_ROLE_ARN,
        config={"name": "TPCDS-q11"},
        job_driver=JOB_DRIVER_ARG_11,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        dag=dag
    )

    job_starter_1 >> job_starter_2 >> job_starter_3 >> job_starter_4 >> job_starter_5 >> job_starter_6 >> job_starter_7 >> job_starter_8 >> job_starter_9 >> job_starter_10 >> job_starter_11

# [END howto_operator_emr_serverless_job]
