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
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

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
checkpoint_location = output_path + "checkpoint/" + args['JOB_NAME'] + "/"

# General Constants
HUDI_FORMAT = "org.apache.hudi"
config = {
    "table_name": "user_order_list",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/user_order_list",
    "primary_key": "id",
    "sort_key": "id",
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

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):

        # dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        logger.info("############  DynamicFrame  ############### \r\n" + getShowString(data_frame, truncate = False))
        # 从Kinesis 获取到通过DMS写入KDS的数据，
        # 包含两个字段
        #   data:实际的数据
        #   metadata:记录这条记录的元数据信息
        #
        schema = StructType([
            StructField("data", StringType(), True),
            StructField("metadata", StringType(), True)
        ])

        # 不转换DynamicFrame，直接使用spark df读取时，一条记录以json格式写入一个字段，默认字段名是 $json$data_infer_schema$_temporary$
        # 对这条记录进行表结构的格式化
        dataJsonDF = data_frame.select(from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias("ds")).select(col("ds.*"))
        logger.info("############  DataSet  ############### \r\n" + getShowString(dataJsonDF, truncate = False))
        dataJsonDF = dataJsonDF.filter("data is not null")
        # sourceJson = dataJsonDF.select('data').first()
        # logger.info("############  Schema Json  ############### \r\n" + getShowString(sourceJson, truncate = False))

        # schemaSource = schema_of_json(sourceJson[0])

        schemaSource = StructType([
            StructField("id", IntegerType(), True),
            StructField("uuid", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("phone_number", StringType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("product_type", StringType(), True),
            StructField("manufacturing_date", IntegerType(), True),
            StructField("price", DecimalType(), True),
            StructField("unit", IntegerType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True)
        ])

        # MutiTable
        dataTables = dataJsonDF.select(from_json(col("data").cast("string"),schemaSource)\
                                       .alias("DATA")).select(col("DATA.*"))
        logger.info("############  Result DF  ############### \r\n" + getShowString(dataTables, truncate = False))

        glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(dataTables, glueContext, "outputDF"),
                                                     connection_type = "custom.spark",
                                                     connection_options = additional_options)



# Read from Kinesis Data Stream from catalog table
sourceData = glueContext.create_data_frame.from_catalog(
    database = "kinesis_db",
    table_name = "kinesis_user_order_list",
    transformation_ctx = "datasource0",
    additional_options = {"startingPosition": "TRIM_HORIZON", "inferSchema": "true"})

glueContext.forEachBatch(frame = sourceData,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()
