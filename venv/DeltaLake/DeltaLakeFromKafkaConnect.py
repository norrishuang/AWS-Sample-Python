import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

config = {
    "database_name": "deltalake",
    "table_name": "delte_portfolio_kc",
    "target": "s3://myemr-bucket-01/data/deltalake/delte_portfolio_kc",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_08"
}

spark = SparkSession.builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').getOrCreate()
glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()

# S3 sink locations
job_time_string = datetime.now().strftime("%Y%m%d%")
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + job_time_string + "/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

additional_options={
    "path": config['target']
}

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

            outputUpsert.write.format("delta") \
                .options(**additional_options) \
                .mode('append') \
                .save()

        if(dataDelete.count() > 0):
            dataDeleteDF = DynamicFrame.fromDF(dataDelete, glueContext, "from_data_frame")
            outputDelete = dataDeleteDF.toDF()
            outputDelete.write.format("delta") \
                .options(**additional_options) \
                .mode('append') \
                .save()


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
