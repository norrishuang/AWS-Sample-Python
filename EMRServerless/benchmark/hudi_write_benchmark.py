
import getopt
import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *

if __name__ == "__main__":

    # print(len(sys.argv))
    if (len(sys.argv) == 0):
        print("Usage: spark-sql-executor [-f sqlfile,-s s3bucket,-h hivevar]")
        sys.exit(0)
    vSQLFile = ''
    vS3Bucket = ''
    vThriftServer = ''

    logger = logging.getLogger()

    opts,args = getopt.getopt(sys.argv[1:], "f:s:h:t:", ["sqlfile=", "s3bucket=", "hivevar=", "thriftserver="])
    for opt_name,opt_value in opts:
        if opt_name in ('-f','--sqlfile'):
            vSQLFile = opt_value
            logger.info("SQLFile:" + vSQLFile)
            print("SQLFile:" + vSQLFile)
        elif opt_name in ('-s','--s3bucket'):
            vS3Bucket = opt_value
            logger.info("S3Bucket:" + vS3Bucket)
            print("S3Bucket:" + vS3Bucket)
        else:
            logger.info("need parameters [sqlfile,s3bucket,hivevar]")
            exit()
    vWarehouse = "s3://" + vS3Bucket + "/warehouse/"
    logger.info("SQL File: " + vSQLFile)
    print("SQL File: " + vSQLFile)
    logger.info("Warehouse location: " + vWarehouse)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", vWarehouse) \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext


    schema = StructType([
        StructField("tablename", StringType(), True),
        StructField("executiontime", DoubleType(), True),
    ])
    resultdf = spark.createDataFrame([], schema)

    ## Table catalog_sales
    table = "catalog_sales"

    SourceData = spark.sql("SELECT * FROM tpcds.catalog_sales")
    write_options = {
        "hoodie.table.name": "catalog_sales",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "BULK_INSERT",
        "hoodie.datasource.write.recordkey.field": "cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk",
        "hoodie.datasource.write.precombine.field": "cs_sold_time_sk",
        "hoodie.datasource.write.partitionpath.field": "cs_sold_date_sk",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.parquet.compression.codec": "snappy",
        "hoodie.populate.meta.fields": "false",
        "hoodie.metadata.enable": "false",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "tpcds_hudi",
        "hoodie.datasource.hive_sync.table": "catalog_sales",
        "hoodie.datasource.hive_sync.partition_fields": "cs_sold_date_sk",
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms"
    }
    start_time = time.time()
    SourceData.write.format("hudi").options(**write_options).mode("append").save("s3://emr-hive-us-east-1-812046859005/banchmark/hudi_warehouse/tpcds_hudi/catalog_sales/")
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"The write data took {execution_time:.2f} seconds to execute.")

    resultdf = resultdf.union(spark.createDataFrame([
        (table, execution_time)
    ], schema))

    resultdf.show()