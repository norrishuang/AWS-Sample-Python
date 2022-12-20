import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

logger.info("Init...")


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

# 处理数据转换精度问题导致的报错
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + job_time_string + "/"


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        # logger.info("Count:%d", data_frame.count())
        # 从kafka拿到的数据是json，需要再转一次dataframe
        df = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame").toDF()

        database_name = config["database_name"]
        table_name = config["table_name"]
        source_table = config["streaming_table"]
        dataUpsert = df.filter((col("op") == "+I") | (col("op") == "+U"))
        dataDelete = df.filter(col("op") == "-D")

        dataUpsertDF = DynamicFrame.fromDF(dataUpsert, glueContext, "from_data_frame")
        dataDeleteDF = DynamicFrame.fromDF(dataDelete, glueContext, "from_data_frame")
        #过滤Insert Updata 的记录

        if(dataUpsertDF.count() > 0):

            dynamic_frame_IU_data = dataUpsertDF.select_fields(paths=["data"])
            #需要做一次格式转换，否者Spark 3.2的特性 SQL ANSI会提示非安全的类型转换
            dynamic_frame_IU = dynamic_frame_IU_data.apply_mapping(
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
            outputDF_Upsert = dynamic_frame_IU.toDF()
            outputDF_Upsert.show()
            outputDF_Upsert.createOrReplaceTempView("tmp_" + source_table + "_upsert")
            query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM tmp_{source_table}_upsert"""
            # query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT * FROM tmp_{source_table}_upsert) u ON t.ID = u.ID
            # WHEN MATCHED THEN UPDATE
            #     SET *
            # WHEN NOT MATCHED THEN INSERT * """
            spark.sql(query)

        if(dataDeleteDF.count() > 0):
            dynamic_frame_D_data = dataDeleteDF.select_fields(paths=["data"])
            dynamic_frame_D = dynamic_frame_D_data.apply_mapping(
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

            outputDF_Delete = dynamic_frame_D.toDF()
            outputDF_Delete.createOrReplaceTempView("tmp_" + source_table + "_delete")
            query = f"""DELETE glue_catalog.{database_name}.{table_name} AS t1 where EXISTS (SELECT ID FROM tmp_{source_table}_delete WHERE t1.ID = ID)"""
            # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}
            spark.sql(query)


# Script generated for node Apache Kafka
dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
    database = config["streaming_db"],
    table_name = config["streaming_table"],
    additional_options = {"startingOffsets": "earliest", "inferSchema": "true"},
    transformation_ctx = "dataframe_ApacheKafka_node1670731139435",
)


glueContext.forEachBatch(frame = dataframe_ApacheKafka_node1670731139435,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})

job.commit()
