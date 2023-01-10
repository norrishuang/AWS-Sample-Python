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
    "table_name": "iceberg_portfolio",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "primary_key": "id",
    "sort_key": "id",
    "commits_to_retain": "4",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_json"
}

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", config['warehouse'])
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
spark.conf.set("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# 处理数据转换精度问题导致的报错
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" +  job_time_string + "/"


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame").apply_mapping(
            [
                ("id", "String", "id", "Integer"),
                ("reward", "String", "reward", "Integer"),
                ("channels", "String", "channels", "String"),
                ("difficulty", "String", "difficulty", "Integer"),
                ("duration", "String", "duration", "Integer"),
                ("offer_type", "String", "offer_type", "String"),
                ("offer_id", "String", "offer_id", "String")
            ]
        )
        # glueContext.write_data_frame.from_catalog(
        #     frame=data_frame,
        #     database=config['database_name'],
        #     table_name=config['table_name']
        # )
        database_name = config['database_name']
        table_name = config['table_name']
        source_table = config['streaming_table']

        outputDF = dynamic_frame.toDF()
        outputDF.createOrReplaceTempView("tmp_" + source_table)

        # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}


        query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM tmp_{source_table}"""
        # query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT * FROM tmp_{source_table}) u ON t.ID = u.ID
        # WHEN MATCHED THEN UPDATE
        #     SET *
        # WHEN NOT MATCHED THEN INSERT *"""
        spark.sql(query)


# Script generated for node Apache Kafka
dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
    database=config['streaming_db'],
    table_name=config['streaming_table'],
    additional_options={"startingOffsets": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_ApacheKafka_node1670731139435",
)


glueContext.forEachBatch(frame = dataframe_ApacheKafka_node1670731139435,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})

job.commit()
