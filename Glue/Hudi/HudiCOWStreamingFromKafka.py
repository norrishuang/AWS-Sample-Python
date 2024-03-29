import sys
from datetime import datetime
import boto3

from pyspark.sql import DataFrame, Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# # job = Job(glueContext)
# conf = SparkConf()
# conf.set('spark.serializer','org.apache.spark.serializer.KryoSerializer')
# sc = SparkContext(conf)

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
    "table_name": "hudi_portfolio",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/hudi_portfolio",
    "primary_key": "id",
    "sort_key": "id",
    "commits_to_retain": "4"
}

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%H%M%S")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/"


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


# Script generated for node Apache Kafka
# 需要验证 hudi delete的能力
dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
    database="kafka_db",
    table_name="kafka_portfolio_json",
    additional_options={"startingOffsets": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_ApacheKafka_node1670731139435",
)


glueContext.forEachBatch(frame = dataframe_ApacheKafka_node1670731139435,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()
