import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

config = {
    "table_name": "iceberg_portfolio_kc",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "dynamic_lock_table": "datacoding_iceberg_lock_table",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_08"
}


spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
    .config("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table") \
    .config("spark.sql.ansi.enabled", "true") \
    .config("spark.sql.storeAssignmentPolicy", "ANSI") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").getOrCreate()

# .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
# .config("spark.sql.catalog.glue_catalog.lock.table", config['dynamic_lock_table']) \


glueContext = GlueContext(spark.sparkContext)
#
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
#
# spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", config['warehouse'])
# spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
# # 处理数据转换精度问题导致的报错
# spark.conf.set("spark.sql.ansi.enabled", "true")
# spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
logger=glueContext.get_logger()

logger.info("Init...")

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + job_time_string + "/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):

        database_name = config["database_name"]
        table_name = config["table_name"]
        source_table = config["streaming_table"]

        logger.info("############  Source DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))

        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])

        data_frame = data_frame.select(from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias("data")).select(col("data.*"))

        logger.info("############  Create DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))

        schemaData = StructType([
            StructField("id", IntegerType(), True),
            StructField("reward", IntegerType(), True),
            StructField("channels", StringType(), True),
            StructField("difficulty", IntegerType(), True),
            StructField("duration", IntegerType(), True),
            StructField("offer_type", StringType(), True),
            StructField("offer_id", StringType(), True)
        ])
        # 过滤 区分 insert upsert delete
        # 这里需要有一个解套的操作，由于change_log是一个嵌套的json，需要通过字段的schema，对dataframe进行解套。
        # spark.read.json(sc.parallelize(data))

        dataUpsert = data_frame.filter("op in ('c','r','u') and after is not null") \
            .select(from_json(col("after").cast("string"),schemaData).alias("DFADD")) \
            .select(col("DFADD.*"))

        dataDelete = data_frame.filter("op in ('d') and before is not null") \
            .select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")) \
            .select(col("DFDEL.*"))
        logger.info("############  Filter dataUpsert DataFrame  ############### \r\n" + getShowString(dataUpsert,truncate = False))

        if(dataUpsert.count() > 0):
            dataUpsertDF = DynamicFrame.fromDF(dataUpsert, glueContext, "from_data_frame")
            outputUpsert = dataUpsertDF.toDF()
            logger.info("##############   After Select Fields  ############# \r\n"
                + getShowString(outputUpsert,truncate = False))

            TempTable = "tmp_" + source_table + "_upsert"
            outputUpsert.createOrReplaceTempView(TempTable)
            query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM {TempTable}"""
            # query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT * FROM tmp_{source_table}_upsert) u ON t.ID = u.ID
            # WHEN MATCHED THEN UPDATE
            #     SET *
            # WHEN NOT MATCHED THEN INSERT * """
            print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
            print(spark.conf.get("spark.sql.catalog.glue_catalog"))
            print("###############################################")
            # outputUpsert.write.format("iceberg").mode("append").save(f"""glue_catalog.{database_name}.{table_name}""")
            outputUpsert.writeTo(f"""glue_catalog.{database_name}.{table_name}""").createOrReplace()
            # spark.sql(query)
        # if(dataDelete.count() > 0):
        #     dataDeleteDF = DynamicFrame.fromDF(dataDelete, glueContext, "from_data_frame")
        #     outputDelete = dataDeleteDF.toDF()
        #     outputDelete.createOrReplaceTempView("tmp_" + source_table + "_delete")
        #     query = f"""DELETE glue_catalog.{database_name}.{table_name} AS t1 where EXISTS (SELECT ID FROM tmp_{source_table}_delete WHERE t1.ID = ID)"""
        #     # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}
        #     spark.sql(query)


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
