import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
from pyspark.sql.functions import col, from_json, schema_of_json,current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
config = {
    "database_name": "iceberg_db",
    "warehouse": "s3://myemr-bucket-01/data/iceberg-folder/",
    "dynamic_lock_table": "datacoding_iceberg_lock_table",
    "streaming_db": "kafka_db",
    "streaming_table": "kafka_user_order"
}

#源表对应iceberg目标表（多表处理）
tableIndexs = {
    "portfolio": "iceberg_portfolio_10",
    "table02": "table02",
    "table01": "table01",
    "user_order": "user_order_main",
    "tb_schema_evolution": "tb_schema_evolution"
}

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog","org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", config['warehouse']) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.sql.storeAssignmentPolicy", "ANSI") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").getOrCreate()

glueContext = GlueContext(spark.sparkContext)
sc = spark.sparkContext

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


logger=glueContext.get_logger()

# S3 sink locations
checkpoint_location = args["TempDir"] + "/" + args['JOB_NAME'] + "/checkpoint/"

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
            "s3://cdc-target-812046859005-us-east-1/topics/"
        ],
        "recurse": True,
    },
    transformation_ctx="SourceDF",
)

def processBatch(data_frame):
    if (data_frame.count() > 0):

        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])

        dataJsonDF = data_frame.select(from_json(col("col0").cast("string"), schema).alias("data")).select(col("data.*"))
        # logger.info("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF,truncate = False))

        dataInsert = dataJsonDF.filter("op in ('c','r') and after is not null")
        # 过滤 区分 insert upsert delete
        dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataInsert.count() > 0):
            #### 分离一个topics多表的问题。
            sourceJson = dataInsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataInsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
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
                logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpsert.count() > 0):
            #### 分离一个topics多表的问题。
            sourceJson = dataUpsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            dataTables = dataUpsert.select(from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"),col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowTables = dataTables.collect()

            for cols in rowTables :
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"), \
                                           from_json(col("source").cast("string"),schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('after').first()
                schemaData = schema_of_json(dataJson[0])

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"),schemaData).alias("DFADD")).select(col("DFADD.*"), current_timestamp().alias("ts"))
                # logger.info("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                MergeIntoDataLake(tableName, dataDFOutput)


        if(dataDelete.count() > 0):
            sourceJson = dataUpsert.select('source').first()
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
                logger.info("############  DELETE FROM  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                DeleteDataFromDataLake(tableName,dataDFOutput)

def InsertDataLake(tableName,dataFrame):

    logger.info("##############  Func:InputDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame,truncate = False))

    database_name = config["database_name"]
    table_name = tableIndexs[tableName]
    dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    # TempTable = "tmp_" + tableName + "_insert"
    # dyDataFrame.createOrReplaceTempView(TempTable)

    # dyDataFrame = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame").toDF();
    dyDataFrame.writeTo(f"glue_catalog.{database_name}.{table_name}")\
        .option("merge-schema", "true")\
        .option("check-ordering","false").append()
    # query = f"""INSERT INTO glue_catalog.{database_name}.{table_name}  SELECT * FROM {TempTable}"""
    # logger.info("####### Execute SQL:" + query)
    # spark.sql(query)
def MergeIntoDataLake(tableName,dataFrame):

    logger.info("##############  Func:MergeIntoDataLake [ "+ tableName +  "] ############# \r\n"
                + getShowString(dataFrame,truncate = False))

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
    # {"data":{"id":1,"reward":10,"channels":"['email', 'mobile', 'social']","difficulty":"10","duration":"7","offer_type":"bogo","offer_id":"ae264e3637204a6fb9bb56bc8210ddfd"},"op":"+I"}
    spark.sql(query)

processBatch(SourceDF)

job.commit()
