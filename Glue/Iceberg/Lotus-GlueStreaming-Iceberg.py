#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


import sys
import boto3
import json
import logging
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkConf
from pyspark.sql import DataFrame, Row
from pyspark.sql import SparkSession
from awsglue import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, LongType
from pyspark.sql.functions import from_json, col, to_json, json_tuple
from pyspark.sql.functions import current_timestamp, unix_timestamp, from_unixtime, to_date
from pyspark.sql.functions import lit, date_format
from pyspark.sql.types import TimestampType

def get_secret():
    secret_name = "prod/redshift"
    region_name = "eu-central-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

secret_dict = json.loads(get_secret())
print("---------------start")
params = [
    'JOB_NAME',
    'TempDir',
    'kafka_broker',
    'topic',
    'startingOffsets',
    'checkpoint_interval',
    'checkpoint_location',
    'aws_region',
    'catalog',
    'database_name',
    'table_name',
    'iceberg_s3_path'
]
args = getResolvedOptions(sys.argv, params)

# Set up Iceberg configuration
CATALOG = args['catalog']
ICEBERG_S3_PATH = args['iceberg_s3_path']
DATABASE = args['database_name']
TABLE_NAME = args['table_name']

spark = SparkSession.builder \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG}","org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_S3_PATH) \
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.ansi.enabled","false") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)
logger = logging.getLogger("mylogger")
logger.setLevel(logging.INFO)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark for Iceberg
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")

# Get job parameters
job_name = args['JOB_NAME']
kafka_broker = args['kafka_broker']
topic = args['topic']
startingOffsets = args['startingOffsets']
checkpoint_interval = args['checkpoint_interval']
checkpoint_location = args['checkpoint_location']
aws_region = args['aws_region']

# Get Kafka credentials from secrets
kafka_username = secret_dict["kafka_username"]
kafka_password = secret_dict["kafka_password"]

# Set up Kafka reader
reader = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("maxOffsetsPerTrigger", "1000000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .option("failOnDataLoss", "false")

if startingOffsets == "earliest" or startingOffsets == "latest":
    reader.option("startingOffsets", startingOffsets)
else:
    reader.option("startingTimestamp", startingOffsets)

# create table
# 在 job.init 之后，process_batch 之前创建表
try:
    creattbsql = f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{DATABASE}.{TABLE_NAME} (
      kafka_time TIMESTAMP
    )
    USING iceberg 
    PARTITIONED BY (days(kafka_time))
    TBLPROPERTIES (
      'write.distribution-mode'='hash',
      'write.metadata.delete-after-commit.enabled'='true',
      'write.metadata.previous-versions-max'='3',
      'write.spark.accept-any-schema'='true'
    )"""

    spark.sql(creattbsql)
    print(f"Table {CATALOG}.{DATABASE}.{TABLE_NAME} created or already exists")
except Exception as e:
    print(f"Error creating table: {str(e)}")

# ...
# Load data from Kafka
kafka_data = reader.load()
df = kafka_data.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS BIGINT)")

def process_batch(data_frame, batchId):
    dfc = data_frame.cache()
    print(job_name + " - my_log - process batch id: " + str(batchId) + " record number: " + str(dfc.count()))

    if not data_frame.rdd.isEmpty():
        try:
            json_schema = spark.read.json(dfc.rdd.map(lambda p: str(p["value"]))).schema
            df_rename = dfc.withColumnRenamed("value", "kafka_data")

            # 将 kafka_time 从 BIGINT 转换为 TIMESTAMP
            df_with_timestamp = df_rename.withColumnRenamed("timestamp", "kafka_time") \
                .withColumn("kafka_time", (col("kafka_time")).cast(TimestampType()))

            final_df = df_with_timestamp

            # 写入 Iceberg 表
            final_df.writeTo(f"{CATALOG}.{DATABASE}.{TABLE_NAME}") \
                .option("merge-schema", "true") \
                .option("check-ordering", "false").append()

            print(job_name + " - my_log - successfully wrote batch to Iceberg table")
        except Exception as e:
            print(job_name + " - my_log - error processing batch: " + str(e))
            print(str(e))

    dfc.unpersist()

# Start the streaming job
save_to_iceberg = df \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="{0} seconds".format(checkpoint_interval)) \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

save_to_iceberg.awaitTermination()
