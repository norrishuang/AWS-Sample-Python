import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext

from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

config = {
    "table_name": "iceberg_portfolio_kc",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "dynamic_lock_table": "datacoding_iceberg_lock_table",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_portfolio_topics"
}

#源表对应iceberg目标表（多表处理）
tableIndexs = {
    "portfolio": "iceberg_portfolio_10",
    "portfolio_01": "portfolio_01",
    "table02": "table02",
    "table01": "table01",
    "user_order_list_small_file": "user_order_list_small_file",
    "user_order_list": "user_order_list",
    "user_order_main": "user_order_main",
    "user_order_mor": "user_order_mor",
    "tb_schema_evolution": "tb_schema_evolution"
}


spark = SparkSession.builder \
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")\
    .config("spark.sql.ansi.enabled","false") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone",True)\
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

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
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/" + "checkpoint-06" + "/"

additional_options = {}
# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame,batchId):
    if (data_frame.count() > 0):
        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])

        dataJsonDF = data_frame.select(from_json(col("$json$data_infer_schema$_temporary$").cast("string"), schema).alias("data")).select(col("data.*"))

        dataInsert = dataJsonDF.filter("op in ('c','r') and after is not null")

        dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataInsert.count() > 0):
            sourceJson = dataInsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # MutiTable
            dataTables = dataInsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataInsert.select(col("after"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])
                logger.info("############  Insert Into-GetSchema-FirstRow:" + dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpsert.count() > 0):
            sourceJson = dataUpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")

                ##由于merge into schema顺序的问题，这里schema从表中获取（顺序问题待解决）
                database_name = config["database_name"]
                table_name = tableIndexs[tableName]
                schemaData = spark.table(f"glue_catalog.{database_name}.{table_name}").schema

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                logger.info("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput)


        if(dataDelete.count() > 0):
            sourceJson = dataDelete.select('source').first()
            # schemaData = schema_of_json([rowjson[0]])

            schemaSource = schema_of_json(sourceJson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            # logger.info("############  Auto Schema Recognize  ############### \r\n" + getShowString(dataDelete,truncate = False))

            rowTables = dataTables.collect()
            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataDelete.select(col("before"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                # logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                DeleteDataFromDataLake(tableName,dataDFOutput)

def InsertDataLake(tableName,dataFrame):
    # logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
    #             + getShowString(dataFrame,truncate = False))

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]

    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();

    # dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    dyDataFrame.writeTo(f"glue_catalog.{database_name}.{table_name}") \
        .option("merge-schema", "true") \
        .option("check-ordering","false").append()

def MergeIntoDataLake(tableName,dataFrame):

    # logger.info("##############  Func:MergeIntoDataLake [ "+ tableName +  "] ############# \r\n"
    #             + getShowString(dataFrame,truncate = False))
    database_name = config["database_name"]
    table_name = tableIndexs[tableName]
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();

    TempTable = "tmp_" + tableName + "_upsert"
    dyDataFrame.createOrReplaceTempView(TempTable)

    query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT * FROM {TempTable}) u ON t.ID = u.ID
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

def DeleteDataFromDataLake(tableName,dataFrame):
    database_name = config["database_name"]
    table_name = tableIndexs[tableName]
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    dyDataFrame.createOrReplaceTempView("tmp_" + tableName + "_delete")
    query = f"""DELETE FROM glue_catalog.{database_name}.{table_name} AS t1 where EXISTS (SELECT ID FROM tmp_{tableName}_delete WHERE t1.ID = ID)"""
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
                         options = {
                             "windowSize": "30 seconds",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })

job.commit()
