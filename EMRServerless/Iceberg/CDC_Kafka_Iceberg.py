import sys
import os
from pyspark.sql import SparkSession
import getopt
from pyspark.sql.functions import col, from_json, schema_of_json, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
from urllib.parse import urlparse
import boto3
import json


job_name = "cdc-kafka-iceberg"

## Init
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "o:t:d:w:f:r:k:c:",
                               ["starting_offsets_of_kafka_topic=",
                                "topics=",
                                "icebergdb=",
                                "warehouse=",
                                "tablejsonfile=",
                                "region=",
                                "kafkaserver=",
                                "checkpointpath="])
    for opt_name,opt_value in opts:
        if opt_name in ('-o', '--starting_offsets_of_kafka_topic'):
            STARTING_OFFSETS_OF_KAFKA_TOPIC = opt_value
            print("STARTING_OFFSETS_OF_KAFKA_TOPIC:" + STARTING_OFFSETS_OF_KAFKA_TOPIC)
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
            print("KAFKA_BOOSTRAPSERVER:" + CHECKPOINT_LOCATION)
        else:
            print("need parameters [starting_offsets_of_kafka_topic,topics,icebergdb etc.]")
            exit()
else:
    print("Job failed. Please provided params STARTING_OFFSETS_OF_KAFKA_TOPIC,TOPICS .etc ")
    sys.exit(1)

config = {
    "database_name": DATABASE_NAME,
}

checkpoint_location = CHECKPOINT_LOCATION + "/" + job_name + "/checkpoint/" + "20230517" + "/"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

spark = SparkSession.builder.config('spark.scheduler.mode', 'FAIR').getOrCreate()
sc = spark.sparkContext
log4j = sc._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

kafka_options = {
    "kafka.bootstrap.servers": KAFKA_BOOSTRAPSERVER,
    "subscribe": TOPICS,
    "kafka.consumer.commit.groupid": "group-" + job_name,
    "inferSchema": "true",
    "classification": "json",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 1000,
    "startingOffsets": STARTING_OFFSETS_OF_KAFKA_TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}


def writeJobLogger(logs):
    logger.info(job_name + " [CUSTOM-LOG]:{0}".format(logs))

def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

def load_tables_config(aws_region, config_s3_path):
    o = urlparse(config_s3_path, allow_fragments=False)
    client = boto3.client('s3', region_name=aws_region)
    data = client.get_object(Bucket=o.netloc, Key=o.path.lstrip('/'))
    file_content = data['Body'].read().decode("utf-8")
    json_content = json.loads(file_content)
    return json_content


tables_ds = load_tables_config(REGION, TABLECONFFILE)


#从kafka获取数据
reader = spark \
    .readStream \
    .format("kafka") \
    .options(**kafka_options)

if STARTING_OFFSETS_OF_KAFKA_TOPIC == "earliest" or STARTING_OFFSETS_OF_KAFKA_TOPIC == "latest":
    reader.option("startingOffsets", STARTING_OFFSETS_OF_KAFKA_TOPIC)
else:
    reader.option("startingTimestamp", STARTING_OFFSETS_OF_KAFKA_TOPIC)

kafka_data = reader.load()

source_data = kafka_data.selectExpr("CAST(value AS STRING)")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):

        sparkprocess = data_frame.sparkSession
        schema = StructType([
            StructField("before", StringType(), True),
            StructField("after", StringType(), True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ])

        writeJobLogger("############  Source Data from Kafka  ############### \r\n" + getShowString(data_frame,truncate = False))

        dataJsonDF = data_frame.select(from_json(col("value").cast("string"), schema).alias("data")).select(col("data.*"))
        writeJobLogger("############  Create DataFrame  ############### \r\n" + getShowString(dataJsonDF, truncate=False))

        '''
        由于Iceberg没有主键，需要通过SQL来处理upsert的场景，需要识别CDC log中的 I/U/D 分别逻辑处理
        '''
        dataInsert = dataJsonDF.filter("op in ('r','c') and after is not null")
        # 过滤 区分 insert upsert delete
        dataUpsert = dataJsonDF.filter("op in ('u') and after is not null")

        dataDelete = dataJsonDF.filter("op in ('d') and before is not null")

        if(dataInsert.count() > 0):
            #### 分离一个topics多表的问题。
            # dataInsert = dataInsertDYF.toDF()
            sourceJson = dataInsert.select('source').first()
            schemaSource = schema_of_json(sourceJson[0])

            # 获取多表
            datatables = dataInsert.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            # logger.info("############  MutiTables  ############### \r\n" + getShowString(dataTables,truncate = False))
            rowtables = datatables.collect()

            for cols in rowtables:
                tableName = cols[1]
                dataDF = dataInsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                datajson = dataDF.select('after').first()
                schemadata = schema_of_json(datajson[0])
                writeJobLogger("############  Insert Into-GetSchema-FirstRow:" + datajson[0])

                '''识别时间字段'''

                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                # for cols in dataDFOutput.schema:
                #     if cols.name in ['created_at', 'updated_at']:
                #         dataDFOutput = dataDFOutput.withColumn(cols.name, to_timestamp(col(cols.name)))
                #         writeJobLogger("Covert time type-Column:" + cols.name)
                # dataDFOutput.printSchema()
                # logger.info("############  INSERT INTO  ############### \r\n" + getShowString(dataDFOutput,truncate = False))
                InsertDataLake(tableName, dataDFOutput)

        if(dataUpsert.count() > 0):
            #### 分离一个topics多表的问题。
            sourcejson = dataUpsert.select('source').first()
            schemasource = schema_of_json(sourcejson[0])

            # 获取多表
            datatables = dataUpsert.select(from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()
            writeJobLogger("MERGE INTO Table Names \r\n" + getShowString(datatables, truncate=False))
            rowtables = datatables.collect()

            for cols in rowtables:
                tableName = cols[1]
                dataDF = dataUpsert.select(col("after"),
                                           from_json(col("source").cast("string"), schemasource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")

                writeJobLogger("MERGE INTO Table [" + tableName + "]\r\n" + getShowString(dataDF, truncate=False))
                ##由于merge into schema顺序的问题，这里schema从表中获取（顺序问题待解决）
                database_name = config["database_name"]

                schemadata = sparkprocess.table(f"glue_catalog.{database_name}.{tableName}").schema
                # datajson = dataDF.select('after').first()
                # schemadata = schema_of_json(datajson[0])
                print(schemadata)
                dataDFOutput = dataDF.select(from_json(col("after").cast("string"), schemadata).alias("DFADD")).select(col("DFADD.*"))

                ## 将时间字同步到UTC
                # for cols in dataDFOutput.schema:
                #     if cols.name in ['created_at', 'updated_at']:
                #         dataDFOutput = dataDFOutput.withColumn(cols.name, to_timestamp(col(cols.name)))
                #         writeJobLogger("Covert time type-Column:" + cols.name)

                writeJobLogger("############  MERGE INTO  ############### \r\n" + getShowString(dataDFOutput, truncate=False))
                MergeIntoDataLake(tableName, dataDFOutput, batchId)


        if(dataDelete.count() > 0):
            sourceJson = dataDelete.select('source').first()

            schemaSource = schema_of_json(sourceJson[0])
            dataTables = dataDelete.select(from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                .select(col("SOURCE.db"), col("SOURCE.table")).distinct()

            rowTables = dataTables.collect()
            for cols in rowTables:
                tableName = cols[1]
                dataDF = dataDelete.select(col("before"),
                                           from_json(col("source").cast("string"), schemaSource).alias("SOURCE")) \
                    .filter("SOURCE.table = '" + tableName + "'")
                dataJson = dataDF.select('before').first()

                schemaData = schema_of_json(dataJson[0])
                dataDFOutput = dataDF.select(from_json(col("before").cast("string"), schemaData).alias("DFDEL")).select(col("DFDEL.*"))
                DeleteDataFromDataLake(tableName, dataDFOutput, batchId)

def InsertDataLake(tableName, dataFrame):

    database_name = config["database_name"]
    # partition as id
    ###如果表不存在，创建一个空表
    '''
    如果表不存在，新建。解决在 writeto 的时候，空表没有字段的问题。
    write.spark.accept-any-schema 用于在写入 DataFrame 时，Spark可以自适应字段。
    format-version 使用iceberg v2版本
    '''
    format_version = "2"
    write_merge_mode = "copy-on-write"
    write_update_mode = "copy-on-write"
    write_delete_mode = "copy-on-write"
    timestamp_fields = ""

    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            format_version = item['format-version']
            write_merge_mode = item['write.merge.mode']
            write_update_mode = item['write.update.mode']
            write_delete_mode = item['write.delete.mode']
            if 'timestamp.fields' in item:
                timestamp_fields = item['timestamp.fields']

    if timestamp_fields != "":
        ##Timestamp字段转换
        for cols in dataFrame.schema:
            if cols.name in timestamp_fields:
                dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                writeJobLogger("Covert time type-Column:" + cols.name)

    dyDataFrame = dataFrame.repartition(4, col("id"))

    creattbsql = f"""CREATE TABLE IF NOT EXISTS glue_catalog.{database_name}.{tableName} 
          USING iceberg 
          TBLPROPERTIES ('write.distribution-mode'='hash',
          'format-version'='{format_version}',
          'write.merge.mode'='{write_merge_mode}',
          'write.update.mode'='{write_update_mode}',
          'write.delete.mode'='{write_delete_mode}',
          'write.metadata.delete-after-commit.enabled'='true',
          'write.metadata.previous-versions-max'='10',
          'write.spark.accept-any-schema'='true')"""

    writeJobLogger("####### IF table not exists, create it:" + creattbsql)
    spark.sql(creattbsql)

    dyDataFrame.writeTo(f"glue_catalog.{database_name}.{tableName}") \
        .option("merge-schema", "true") \
        .option("check-ordering", "false").append()

def MergeIntoDataLake(tableName, dataFrame, batchId):

    ## 待研究问题：为什么需要通过DF 才能获取到当前会话的sparkSession
    sparkDF = dataFrame.sparkSession

    database_name = config["database_name"]
    primary_key = 'ID'
    timestamp_fields = ''
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']
            if 'timestamp.fields' in item :
                timestamp_fields = item['timestamp.fields']

    if timestamp_fields != '':
        ##Timestamp字段转换
        for cols in dataFrame.schema:
            if cols.name in timestamp_fields:
                dataFrame = dataFrame.withColumn(cols.name, to_timestamp(col(cols.name)))
                writeJobLogger("Covert time type-Column:" + cols.name)

    TempTable = "tmp_" + tableName + "_upsert_" + str(batchId)
    dataFrame.createOrReplaceTempView(TempTable)

    query = f"""MERGE INTO glue_catalog.{database_name}.{tableName} t USING (SELECT * FROM {TempTable}) u ON t.{primary_key} = u.{primary_key}
            WHEN MATCHED THEN UPDATE
                SET *
            WHEN NOT MATCHED THEN INSERT * """
    logger.info("####### Execute SQL:" + query)
    try:
        sparkDF.sql(query)
    except ValueError:
        writeJobLogger("MergeIntoDataLake Unexpected error:", sys.exc_info()[0])


def DeleteDataFromDataLake(tableName, dataFrame, batchId):
    sparkDF = dataFrame.sparkSession

    database_name = config["database_name"]
    primary_key = 'ID'
    for item in tables_ds:
        if item['db'] == database_name and item['table'] == tableName:
            primary_key = item['primary_key']

    database_name = config["database_name"]
    TempTable = "tmp_" + tableName + "_upsert_" + str(batchId)
    dataFrame.createOrReplaceTempView(TempTable)
    query = f"""DELETE FROM glue_catalog.{database_name}.{tableName} AS t1 
         where EXISTS (SELECT {primary_key} FROM {TempTable} WHERE t1.{primary_key} = {primary_key})"""

    try:
        sparkDF.sql(query)
    except ValueError:
        writeJobLogger("DeleteDataFromDataLake Unexpected error:", sys.exc_info()[0])

source_data \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .foreachBatch(processBatch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()\
    .awaitTermination()
