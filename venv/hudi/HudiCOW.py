
import sys
import os
import json

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, dense_rank, desc
from pyspark.sql.window import Window

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
glueClient = boto3.client('glue')
logger.info('Initialization.')

# General Constants
HUDI_FORMAT = "org.apache.hudi"

config = {
    "table_name": "hudi_trips_table",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/hudi_trips_table",
    "primary_key": "trip_id",
    "sort_key": "tstamp",
    "commits_to_retain": "4"
}

## Generates Data
def get_json_data(start, count, dest):
    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = [{"trip_id": i, "tstamp": time_stamp, "route_id": chr(65 + (i % 10)), "destination": dest[i%10]} for i in range(start, start + count)]
    return data

# Creates the Dataframe
def create_json_df(spark, data):
    sc = spark.sparkContext
    return spark.read.json(sc.parallelize(data))

dest = ["Seattle", "New York", "New Jersey", "Los Angeles", "Las Vegas", "Tucson","Washington DC","Philadelphia","Miami","San Francisco"]
df1 = create_json_df(spark, get_json_data(0, 2000000, dest))

# datarows = "table rows:{0}"
# logger.info(datarows.format(df1.count()))
# logger.info(df1.show())

additional_options={
    "hoodie.table.name": config['table_name'],
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": config["primary_key"],
    "hoodie.datasource.write.precombine.field": config["sort_key"],
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": config['database_name'],
    "hoodie.datasource.hive_sync.table": config['table_name'],
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": config['target']
}

df1.write.format(HUDI_FORMAT).options(**additional_options).mode("append").save()

job.commit()