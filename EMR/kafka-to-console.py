from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import sys
import getopt


# SparkSQL for EMRServerless

WAREHOUSE = 's3://emr-hive-us-east-1-812046859005/datalake/iceberg-folder/'
KAFKA_BOOSTRAPSERVER = 'b-1.kafkatest.pqxcoc.c17.kafka.us-east-1.amazonaws.com:9092,b-3.kafkatest.pqxcoc.c17.kafka.us-east-1.amazonaws.com:9092,b-2.kafkatest.pqxcoc.c17.kafka.us-east-1.amazonaws.com:9092'
TOPICS = 'norrisdb.norrisdb.user_order_list'
JOB_NAME = 'msk-test-cdc-01'
STARTING_OFFSETS_OF_KAFKA_TOPIC = 'earliest'





def processData(data_frame_batch, batchId):

    data_frame = data_frame_batch.cache()
    schema = StructType([
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", StringType(), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("transaction", StringType(), True)
    ])

    dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("data")).select(col("data.*"))

    writeJobLogger("## Source Data from Kafka Batch\r\n + " + getShowString(data_frame, truncate=False))

    writeJobLogger("## Create DataFrame \r\n" + getShowString(dataJsonDF, truncate=False))

if __name__ == "__main__":

    if len(sys.argv) > 1:
        opts, args = getopt.getopt(sys.argv[1:],
                                       "j:o:t:d:w:f:r:k:c:",
                                       ["jobname=",
                                        "starting_offsets_of_kafka_topic=",
                                        "topics=",
                                        "icebergdb=",
                                        "warehouse=",
                                        "tablejsonfile=",
                                        "region=",
                                        "kafkaserver=",
                                        "checkpointpath="])
        for opt_name, opt_value in opts:
            if opt_name in ('-o', '--starting_offsets_of_kafka_topic'):
                STARTING_OFFSETS_OF_KAFKA_TOPIC = opt_value
                print("STARTING_OFFSETS_OF_KAFKA_TOPIC:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
            elif opt_name in ('-j', '--jobname'):
                JOB_NAME = opt_value
                print("JOB_NAME:" + JOB_NAME)
            elif opt_name in ('-t', '--topics'):
                TOPICS = opt_value.replace('"', '')
                print("TOPICS:" + TOPICS)
            elif opt_name in ('-d', '--icebergdb'):
                DATABASE_NAME = opt_value
                print("DATABASE_NAME:" + DATABASE_NAME)
            elif opt_name in ('-w', '--warehouse'):
                WAREHOUSE = opt_value
                print("WAREHOUSE:" + WAREHOUSE)
            elif opt_name in ('-f', '--tablejsonfile'):
                TABLECONFFILE = opt_value
                print("TABLECONFFILE:" + TABLECONFFILE)
            elif opt_name in ('-r', '--region'):
                REGION = opt_value
                print("REGION:" + REGION)
            elif opt_name in ('-k', '--kafkaserver'):
                KAFKA_BOOSTRAPSERVER = opt_value
                print("KAFKA_BOOSTRAPSERVER:" + KAFKA_BOOSTRAPSERVER)
            elif opt_name in ('-c', '--checkpointpath'):
                CHECKPOINT_LOCATION = opt_value
                print("CHECKPOINT_LOCATION:" + CHECKPOINT_LOCATION)
            else:
                print("need parameters [starting_offsets_of_kafka_topic,topics,icebergdb etc.]")
                exit()
    else:
        print("Job failed. Please provided params STARTING_OFFSETS_OF_KAFKA_TOPIC,TOPICS .etc ")
        sys.exit(1)

def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)
def writeJobLogger(logs):
    logger.info(JOB_NAME + " [CUSTOM-LOG]:{0}".format(logs))

spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE) \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", True) \
        .getOrCreate()
sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)



kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOSTRAPSERVER,
    "subscribe": TOPICS,
        # "kafka.consumer.commit.groupid": "group-" + JOB_NAME,
        # "inferSchema": "true",
        # "classification": "json",
        # "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 10000,
    "max.partition.fetch.bytes": 10485760,
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
        # "kafka.security.protocol": "SASL_SSL",
        # "kafka.sasl.mechanism": "AWS_MSK_IAM",
        # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
        # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

reader = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_options)

kafka_data = reader.load()

source_data = kafka_data.selectExpr("CAST(value AS STRING)")

checkpointpath = "s3://emr-hive-us-east-1-812046859005/checkpoint/kafka-to-console/"
source_data \
        .writeStream \
        .outputMode("append") \
        .trigger(processingTime="60 seconds") \
        .foreachBatch(processData) \
        .option("checkpointLocation", checkpointpath) \
        .start() \
        .awaitTermination()