import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger=glueContext.get_logger()


config = {
    "table_name": "iceberg_user_order",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "primary_key": "id",
    "sort_key": "id",
    "commits_to_retain": "4",
    "Streaming_GlueCatalogDB": "kafka_db",
    "Streaming_GlueCatalogTable": "kafka_user_order"
}
#
# spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", config['warehouse'])
# spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table")
# spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%H%M%S")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/"


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        outputDF = dynamic_frame.toDF()
        # glueContext.write_data_frame.from_catalog(
        #     frame=dynamic_frame,
        #     database=config['database_name'],
        #     table_name=config['table_name']
        # )
        outputDF.createOrReplaceTempView("tmp_iceberg_user_order")

        query = f"""
        insert into glue_catalog.iceberg_db.iceberg_user_order
        SELECT * FROM tmp_iceberg_user_order
        """
        spark.sql(query)


# Script generated for node Apache Kafka
dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
    database=config['Streaming_GlueCatalogDB'],
    table_name=config['Streaming_GlueCatalogTable'],
    additional_options={"startingOffsets": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_ApacheKafka_node1670731139435",
)


glueContext.forEachBatch(frame = dataframe_ApacheKafka_node1670731139435,
                         batch_function = processBatch,
                         options = {"windowSize": "100 seconds", "checkpointLocation": checkpoint_location})

job.commit()
