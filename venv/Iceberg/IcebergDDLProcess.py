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

###########################################################
#  处理通过Kafka Connect 输出的 DDL变更，应用到iceberg表中。
#  只针对alter 的语法做处理
###########################################################
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
config = {
    "sourceDBName": "norrisdb",
    "table_name": "iceberg_portfolio_10",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "primary_key": "id",
    "sort_key": "id",
    "dynamic_lock_table": "datacoding_iceberg_lock_table",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_10"
}



spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.ansi.enabled", "true") \
    .config("spark.sql.storeAssignmentPolicy", "ANSI") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").getOrCreate()

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

glueContext = GlueContext(spark.sparkContext)
sc = spark.sparkContext

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


logger=glueContext.get_logger()

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d%H%M%S")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + job_time_string + "/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

#由于读取直接读取JSON文件，无法做到解嵌套，将json文件当作csv文件读取，再做解析
SourceDF = glueContext.create_data_frame_from_options(
    format_options={
        "separator": "|",
        "quoteChar": -1
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-ddl-target-812046859005-us-east-1/topics/norrisdb/"
        ],
        "recurse": True,
    },
    transformation_ctx="SourceDF",
)


logger.info("############  Source DataFrame  ############### \r\n" + getShowString(SourceDF, truncate = False))

def processBatch(data_frame):
    if (data_frame.count() > 0):

        logger.info("############  Source Batch Process DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))

        #第一级 schema
        schemaLevel01 = StructType([
            StructField("databaseName", StringType(), True),
            StructField("tableChanges", StringType(), True),
            StructField("source", StringType(), True),
            StructField("schemaName", StringType(), True),
            StructField("ddl", StringType(), True)
        ])

        dfLevel01 = data_frame.select(from_json(col("col0").cast("string"), schemaLevel01).alias("data")).select(col("data.*"))

        logger.info("############  Level 01  ############### \r\n" + getShowString(data_frame,truncate = False))

        schemaTableChanges = StructType([
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("table", StringType(), True)
        ])

        schemaSource = StructType([
            StructField("query", StringType(), True),
            StructField("thread", StringType(), True),
            StructField("server_id", LongType(), True),
            StructField("version", StringType(), True),
            StructField("sequence", StringType(), True),
            StructField("file", StringType(), True),
            StructField("connector", StringType(), True),
            StructField("pos", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("gtid", StringType(), True),
            StructField("row", IntegerType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("table", StringType(), True)
        ])

        dataTableChanges = dfLevel01.select(col('ddl'),from_json(col("tableChanges").cast("string"),schemaTableChanges).alias("TB"), \
                                        from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
        .filter("tableChanges <> '[]' and databaseName = '" + config['sourceDBName'] + "'").select(col("TB.*"),col("SOURCE.db").alias("dbname"),col("SOURCE.table").alias("tablename"),col("ddl"))

        dataTable = dataTableChanges.select(col('type'),col("dbname"),col("tablename"),col('ddl')) \
            .filter("type = 'ALTER'")
        for row in dataTable.collect() :
            #spark.sql("alter table glue_catalog.iceberg_db.table01 add column c varchar(10)")
            #spark.sql("alter table glue_catalog.iceberg_db.table01 add columns (d varchar(10),e int)")
            DDL = row['ddl']
            spark.sql(query)

processBatch(SourceDF)

job.commit()
