import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.conf import SparkConf
from datetime import datetime
from awsglue import DynamicFrame

## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'catalog',
                                     'database_name',
                                     'table_name',
                                     'primary_key',
                                     'kafka_topic_name',
                                     'starting_offsets_of_kafka_topic',
                                     'kafka_connection_name',
                                     'iceberg_s3_path',
                                     'aws_region',
                                     'window_size'
                                     ])

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
# logger=glueContext.get_logger()

CATALOG = args['catalog']
ICEBERG_S3_PATH = args['iceberg_s3_path']
DATABASE = args['database_name']
TABLE_NAME = args['table_name']
PRIMARY_KEY = args['primary_key']
KAFKA_TOPIC_NAME = args['kafka_topic_name']
KAFKA_CONNECTION_NAME = args['kafka_connection_name']
STARTING_OFFSETS_OF_KAFKA_TOPIC = args.get('starting_offsets_of_kafka_topic', 'latest')

AWS_REGION = args['aws_region']
WINDOW_SIZE = args.get('window_size', '30 seconds')

# config = {
#     "table_name": "iceberg_portfolio",
#     "database_name": "iceberg_db",
#     "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
#     "primary_key": "id",
#     "sort_key": "id",
#     "commits_to_retain": "4",
#     "streaming_db": "kafka_db",
#     "streaming_table": "kafka_portfolio_json"
# }


def setSparkIcebergConf() -> SparkConf:
    conf_list = [
        (f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog"),
        (f"spark.sql.catalog.{CATALOG}.warehouse", ICEBERG_S3_PATH),
        (f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog"),
        (f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"),
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        ("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    ]
    spark_conf = SparkConf().setAll(conf_list)
    return spark_conf

conf = setSparkIcebergConf()
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 处理数据转换精度问题导致的报错
spark.conf.set("spark.sql.ansi.enabled", "true")
spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")

##Glue Catalog 还不支持配置 KAFKA IAM 认证，通过如下方式，可以对接到Kafka Serverless
kafka_options = {
    "connectionName": KAFKA_CONNECTION_NAME,
    "topicName": KAFKA_TOPIC_NAME,
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    "inferSchema": "true",
    "classification": "json",

    #XXX: the properties below are required for IAM Access control for MSK Serverless
    # "kafka.security.protocol": "SASL_SSL",
    # "kafka.sasl.mechanism": "AWS_MSK_IAM",
    # "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    # "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}


# job_time_string = datetime.now().strftime("%Y%m%d")
# s3_target = output_path + job_time_string
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/"


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
        # database_name = config['database_name']
        # table_name = config['table_name']
        # source_table = config['streaming_table']

        outputDF = dynamic_frame.toDF()
        outputDF.createOrReplaceTempView("tmp_" + TABLE_NAME)

        # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}


        query = f"""INSERT INTO {CATALOG}.{DATABASE}.{TABLE_NAME}  SELECT * FROM tmp_{TABLE_NAME}"""
        # query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT * FROM tmp_{source_table}) u ON t.ID = u.ID
        # WHEN MATCHED THEN UPDATE
        #     SET *
        # WHEN NOT MATCHED THEN INSERT *"""
        spark.sql(query)


# Script generated for node Apache Kafka
# dataframe_ApacheKafka_node1670731139435 = glueContext.create_data_frame.from_catalog(
#     database=config['streaming_db'],
#     table_name=config['streaming_table'],
#     additional_options={"startingOffsets": "earliest", "inferSchema": "true"},
#     transformation_ctx="dataframe_ApacheKafka_node1670731139435",
# )

streaming_data = glueContext.create_data_frame.from_options(
    connection_type="kafka",
    connection_options=kafka_options,
    transformation_ctx="kafka_df"
)


glueContext.forEachBatch(frame = streaming_data,
                         batch_function = processBatch,
                         options = {"windowSize": WINDOW_SIZE, "checkpointLocation": checkpoint_location})

job.commit()
