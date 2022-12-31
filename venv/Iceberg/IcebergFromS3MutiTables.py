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

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
config = {
    "table_name": "iceberg_portfolio_10",
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "primary_key": "id",
    "sort_key": "id",
    "dynamic_lock_table": "datacoding_iceberg_lock_table",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_user_order",
    "kafka.topics": "demodb10.demo.portfolio"
}

#源表对应iceberg目标表（多表处理）
tableIndexs = {
    "portfolio": "iceberg_portfolio_10",
    "table02": "table02",
    "table01": "table01",
    "user_order": "user_order"
}




spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.ansi.enabled", "true") \
    .config("spark.sql.storeAssignmentPolicy", "ANSI") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").getOrCreate()

# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

glueContext = GlueContext(spark.sparkContext)
sc = spark.sparkContext

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


logger=glueContext.get_logger()

# spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://myemr-bucket-01/data/iceberg-folder/")
# spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
# spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
# spark.conf.set("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table")

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


#由于读取直接读取JSON文件，无法做到解嵌套，将json文件当作csv文件读取，再做解析(多个topic)
SourceDF = glueContext.create_data_frame_from_options(
    format_options={
        "separator": "|",
        "quoteChar": -1
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://cdc-target-812046859005-us-east-1/topics/norrisdb.norrisdb.user_order/"
        ],
        "recurse": True,
    },
    transformation_ctx="SourceDF",
)


logger.info("############  Source DataFrame  ############### \r\n" + getShowString(SourceDF, truncate = False))

def processBatch(data_frame):
    if (data_frame.count() > 0):
        #dynamicDF转成DF（直接转换会丢失json的数据结构）
        # data_frame = data_frame.toDF()

        # database_name = config["database_name"]
        # table_name = config["table_name"]
        # source_table = config["streaming_table"]

        logger.info("############  Source Batch Process DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))

        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])




        data_frame = data_frame.select(from_json(col("col0").cast("string"), schema).alias("data")).select(col("data.*"))
        logger.info("############  Create DataFrame  ############### \r\n" + getShowString(data_frame,truncate = False))



        # 过滤 区分 insert upsert delete
        # 这里需要有一个解套的操作，由于change_log是一个嵌套的json，需要通过字段的schema，对dataframe进行解套。
        dataUpsert = data_frame.filter("op in ('c','r','u') and after is not null")

        # dataDelete = data_frame.filter("op in ('d') and before is not null") \
        dataDelete = data_frame.filter("op in ('d') and before is not null")

        if(dataUpsert.count() > 0):
            #根据DataFrame自动生成schema，需要对发生DDL（add/drop column 时做出响应）
            #获取一个DF的最后一行作为schema的基准
            #### 分离一个topics多表的问题。

            sourceJson = dataUpsert.select('source').first()
            schemaSource = spark.read.json(sc.parallelize([sourceJson[0]])).schema

            # 获取多表
            dataTables = dataUpsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE"))\
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"),\
                    from_json(col("source").cast("string"),schemaSource).alias("SOURCE"))\
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').collect()[dataDF.count()-1]
                schemaData = spark.read.json(sc.parallelize([dataJson[0]])).schema

                database_name = config["database_name"]
                table_name = tableIndexs[tableName]
                # schemaTable = spark.table(f"""glue_catalog.{database_name}.{table_name}""").schema

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"))
                logger.info("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InputDataLake(tableName, dataDFOutput)


        if(dataDelete.count() > 0):
            rowjson = dataDelete.select('before').collect()[dataUpsert.count()-1]

            schemaSource = spark.read.json(sc.parallelize([sourceJson[0]])).schema

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
                schemaData = spark.read.json(sc.parallelize([dataJson[0]])).schema
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"),schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                DeleteDataFromDataLake(tableName,dataDFOutput)


def InputDataLake(tableName,dataFrame):
    # dataUpsertDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
    # outputUpsert = dataUpsertDF.toDF()
    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame,truncate = False))

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]

    TempTable = "tmp_" + tableName + "_upsert"
    dataFrame.createOrReplaceTempView(TempTable)
    # query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM {TempTable}"""
    query = f"""MERGE INTO glue_catalog.{database_name}.{table_name} t USING (SELECT *,current_timestamp() as ts FROM {TempTable}) u ON t.ID = u.ID
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    spark.sql(query)

def DeleteDataFromDataLake(tableName,dataFrame):
    database_name = config["database_name"]
    table_name = tableIndexs[tableName]

    dataFrame.createOrReplaceTempView("tmp_" + tableName + "_delete")
    query = f"""DELETE FROM glue_catalog.{database_name}.{table_name} AS t1 where EXISTS (SELECT ID FROM tmp_{tableName}_delete WHERE t1.ID = ID)"""
    # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}
    spark.sql(query)

processBatch(SourceDF)

job.commit()
