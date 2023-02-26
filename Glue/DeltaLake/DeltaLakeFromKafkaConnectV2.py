import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

config = {
    "database_name": "deltalakedb",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_norrisdb_table_02",
    "target": "s3://myemr-bucket-01/data/deltalakedb/table02",
}

additional_options={
    "path": config['target'],
    "mergeSchema": "true"
}

#源表对应iceberg目标表（多表处理）
tableIndexs = {
    "portfolio": "iceberg_portfolio_10",
    "table02": "table02",
    "table01": "table01",
    "user_order": "user_order"
}

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

#
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger=glueContext.get_logger()

logger.info("Init...")

# S3 sink locations
job_time_string = datetime.now().strftime("%Y%m%d%")
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/"

# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):

        logger.info("############  Source Batch Process DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))

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

        # 过滤 区分 insert upsert delete
        # 这里需要有一个解套的操作，由于change_log是一个嵌套的json，需要通过字段的schema，对dataframe进行解套。
        # dataInsert = data_frame.filter("op in ('c','r') and after is not null")

        dataUpsert = data_frame.filter("op in ('c','r','u') and after is not null")

        dataDelete = data_frame.filter("op in ('d') and before is not null")


        # if(dataInsert.count() > 0):
        #     #根据DataFrame自动生成schema，需要对发生DDL（add/drop column 时做出响应）
        #     #获取一个DF的最后一行作为schema的基准
        #     #### 分离一个topics多表的问题。
        #     sourceJson = dataInsert.select('source').first()
        #     schemaSource = schema_of_json(sourceJson[0])
        #
        #     # 获取多表
        #     dataTables = dataInsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
        #         .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
        #     logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
        #     rowTables = dataTables.collect()
        #
        #     for cols in rowTables :
        #         tableName = cols[1]
        #         dataDF = dataInsert.select(col("after"), \
        #                                    from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
        #             .filter("SOURCE.table = '" + tableName + "'")
        #         dataJson = dataDF.select('after').collect()[dataDF.count()-1]
        #         schemaData = schema_of_json(dataJson[0])
        #
        #         dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"))
        #         logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
        #         dyDataFrame = DynamicFrame.fromDF(dataDFOutput, glueContext, "from_data_frame")
        #         InsertDataLake(tableName, dyDataFrame)

        if(dataUpsert.count() > 0):
            #根据DataFrame自动生成schema，需要对发生DDL（add/drop column 时做出响应）
            #获取一个DF的最后一行作为schema的基准
            #### 分离一个topics多表的问题。

            sourceJson = dataUpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').collect()[dataDF.count()-1]
                schemaData = schema_of_json(dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"))
                logger.info("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                # dataDFOutput.createOrReplaceTempView("temp_table")
                # queryDF = spark.sql(f"""select * from temp_table""")
                # logger.info("############  temp table  ############### \r\n" + getShowString(queryDF,truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput)


        if(dataDelete.count() > 0):
            rowjson = dataDelete.select('before').collect()[dataUpsert.count()-1]
            schemaSource = schema_of_json(rowjson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()

            logger.info("############  Auto Schema Recognize  ############### \r\n" + getShowString(dataDelete,truncate = False))

            rowTables = dataTables.collect()
            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataDelete.select(col("before"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').collect()[dataDF.count()-1]
                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                DeleteDataFromDataLake(tableName,dataDFOutput)

# Insert
# def InsertDataLake(tableName,dyDataFrame):
#
#     database_name = config["database_name"]
#     table_name = tableIndexs[tableName]
#     writeDF = dyDataFrame.toDF();
#     writeDF.write \
#         .format("delta") \
#         .options(**additional_options) \
#         .option("mergeSchema", "true") \
#         .mode("append") \
#         .save()
#     # queryDF = spark.sql(f"""select * from {TempTable}""")
#     # logger.info("##############  Func:Temp Table [ temp_table] ############# \r\n"
#     #             + getShowString(queryDF,truncate = False))
#     # # query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM {TempTable}"""
#     # query = f"""INSERT INTO {database_name}.{table_name} select * from {TempTable}"""
#     # logger.info("####### Execute SQL:" + query)
#     # spark.sql(query)


def MergeIntoDataLake(tableName,dataFrame):
    # dataUpsertDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    # outputUpsert = dataUpsertDF.toDF()
    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame,truncate = False))

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]


    #需要做一次转换，不然spark session获取不到
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    TempTable = "tmp_" + tableName + "_upsert"
    dyDataFrame.createOrReplaceTempView(TempTable)
    queryDF = spark.sql(f"""select * from {TempTable}""")
    logger.info("##############  Func:Temp Table [ temp_table] ############# \r\n"
                + getShowString(queryDF,truncate = False))
    # query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM {TempTable}"""
    query = f"""MERGE INTO {database_name}.{table_name} t USING (select * from {TempTable}) u ON t.ID = u.ID
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

def DeleteDataFromDataLake(tableName,dataFrame):
    database_name = config["database_name"]
    table_name = tableIndexs[tableName]

    dataFrame.createOrReplaceTempView("tmp_" + tableName + "_delete")
    query = f"""DELETE FROM {database_name}.{table_name} AS t1 where EXISTS (SELECT ID FROM tmp_{tableName}_delete WHERE t1.ID = ID)"""
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
                         options = {
                             "windowSize": "30 seconds",
                             "checkpointLocation": checkpoint_location,
                             "batchMaxRetries": 1
                         })




job.commit()
