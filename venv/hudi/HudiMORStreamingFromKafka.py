import sys
from datetime import datetime
from pyspark.sql.session import SparkSession

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
logger.info('Initialization.')

# General Constants
HUDI_FORMAT = "org.apache.hudi"
config = {
    "table_name": "hudi_portfolio_mor",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/hudi_portfolio_mor",
    "primary_key": "id",
    "sort_key": "id",
    "commits_to_retain": "4",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_json"
}

# S3 sink locations
output_path = "s3://myemr-bucket-01/data/"
job_time_string = datetime.now().strftime("%Y%m%d")
s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + job_time_string + "/"

additional_options={
    "hoodie.table.name": config['table_name'],
    "hoodie.datasource.write.storage.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": config["primary_key"],
    "hoodie.datasource.write.precombine.field": config["sort_key"],
    "hoodie.bulkinsert.shuffle.parallelism" : 3,
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.datasource.hive_sync.database": config['database_name'],
    "hoodie.datasource.hive_sync.table": config['table_name'],
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.compact.inline.max.delta.commits": 3,
    "path": config['target']
}


def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):

        logger.info("############  Source DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))
        # logger.info("### \r\n" + getShowString(data_frame,truncate = False))

        schema = StructType([
            StructField("data", StringType(), True),
            StructField("op", StringType(), True)
        ])
        data_frame.printSchema()
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
        dataUpsert = data_frame.filter((col("op") == "+I") | (col("op") == "+U")).select(from_json(col("data").cast("string"),schemaData).alias("df")).select(col("df.*"))
        dataDelete = data_frame.filter(col("op") == "-D").select(col("data")).select(from_json(col("data").cast("string"),schemaData).alias("df")).select(col("df.*"))


        logger.info("############  Filter dataUpsert DataFrame  ############### \r\n" + getShowString(dataUpsert,truncate = False))
        # database_name = config["database_name"]
        # table_name = config["table_name"]
        # source_table = config["streaming_table"]
        # dataUpsertDF = df.filter(f=lambda x: x["op"] in ["+U", "+I"])
        # dataDeleteDF = df.filter(f=lambda x: x["op"] in ["-D"])


        if (dataUpsert.count() > 0):
            dataUpsertDF = DynamicFrame.fromDF(dataUpsert, glueContext, "from_data_frame")
            # dataUpsertSelect = dataUpsertDF.select_fields(paths=["data"])
            outputUpsert = dataUpsertDF.toDF()
            logger.info("##############   After Select Fields  ############# \r\n"
                        + getShowString(outputUpsert,truncate = False))
            outputUpsert.write.format(HUDI_FORMAT).options(**additional_options).mode("append").save()

        if (dataDelete.count() > 0):
            dataUpsertDF = DynamicFrame.fromDF(dataDelete, glueContext, "from_data_frame")
            # dataDeleteSelect = dataDelete.select_fields(paths=["data"])
            outputDelete = dataUpsertDF.toDF()
            # outputDFDelete.show()
            outputDelete.write.format(HUDI_FORMAT).options(**additional_options)\
                .option("hoodie.datasource.write.operation", "delete")\
                .mode("append").save()

#{"data":{"id":6,"reward":5,"channels":"['web', 'email']","difficulty":"20","duration":"10","offer_type":"discount","offer_id":"0b1e1539f2cc45b7b9fa7c272da2e1d7"},"op":"-D"}

# Script generated for node Apache Kafka  latest/earliest
dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
    database="kafka_db",
    table_name="kafka_portfolio_json",
    additional_options={"startingOffsets": "earliest", "inferSchema": "true", "classification": "json"},
    transformation_ctx="dataframe_ApacheKafka_node1670731139435",
)


def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

glueContext.forEachBatch(frame = dataframe_ApacheKafka_node1670731139435,
                         batch_function = processBatch,
                         options = {"windowSize": "60 seconds", "checkpointLocation": checkpoint_location})
job.commit()
