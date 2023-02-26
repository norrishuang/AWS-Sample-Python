import sys
from datetime import datetime
import boto3
import base64
from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
glueClient = boto3.client('glue')
logger.info('Initialization.')


# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%H%M%S")
s3_target = output_path + job_time_string
checkpoint_location = output_path + "checkpoint/"
temp_path = output_path + "temp/"

# General Constants
HUDI_FORMAT = "org.apache.hudi"
config = {
    "table_name": "hudi_ev_station_data",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/hudi_ev_station_data",
    "primary_key": "_id",
    "sort_key": "connectionTime",
    "commits_to_retain": "4"
}

additional_options={
    "hoodie.table.name": config['table_name'],
    "className" : "org.apache.hudi",
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


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        outputDF = dynamic_frame.toDF()

        glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(outputDF, glueContext, "outputDF"),
                                                     connection_type = "custom.spark",
                                                     connection_options = additional_options)



# Read from Kinesis Data Stream from catalog table
sourceData = glueContext.create_data_frame.from_catalog(
    database = "kinesis_db",
    table_name = "kinesis_ev_station_data",
    transformation_ctx = "datasource0",
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

glueContext.forEachBatch(frame = sourceData,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()
