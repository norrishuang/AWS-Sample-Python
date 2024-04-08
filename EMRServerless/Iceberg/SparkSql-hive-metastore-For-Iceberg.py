import getopt
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# SparkSQL for EMRServerless

if __name__ == "__main__":

    # print(len(sys.argv))
    if (len(sys.argv) == 0):
        print("Usage: spark-sql-executor [-f sqlfile,-s s3bucket,-h hivevar,-d database,-m hivemetastore]")
        sys.exit(0)
    vSQLFile = ''
    vS3Bucket = ''
    vHiveMetastore = ''

    logger = logging.getLogger()

    database = 'default'
    opts,args = getopt.getopt(sys.argv[1:], "f:s:h:d:m:", ["sqlfile=", "s3bucket=", "hivevar=", "hivemetastore="])
    for opt_name,opt_value in opts:
        if opt_name in ('-f', '--sqlfile'):
            vSQLFile = opt_value
            logger.info("SQLFile:" + vSQLFile)
            print("SQLFile:" + vSQLFile)
        elif opt_name in ('-s', '--s3bucket'):
            vS3Bucket = opt_value
            logger.info("S3Bucket:" + vS3Bucket)
            print("S3Bucket:" + vS3Bucket)
        elif opt_name in ('-h', '--hivevar'):
            hivevar = opt_value
            exec(hivevar)
            print("hivevar:" + hivevar)
        elif opt_name in ('-m', '--hivemetastore'):
            vHiveMetastore = opt_value
            print("hive_metastore:" + vHiveMetastore)
        elif opt_name in ('-d', '--database'):
            database = opt_value
            print("database:" + database)
        else:
            logger.info("need parameters [sqlfile, s3bucket, hivevar or database]")
            exit()
    vWarehouse = "s3://" + vS3Bucket + "/warehouse/"
    logger.info("SQL File: " + vSQLFile)
    print("SQL File: " + vSQLFile)
    logger.info("Warehouse location: " + vWarehouse)

    spark = SparkSession \
        .builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive_prod.warehouse", vWarehouse) \
        .config("spark.sql.catalog.hive_prod.type", "hive") \
        .config("spark.sql.catalog.hive_prod.uri", vHiveMetastore) \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", True) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    rdd = sc.wholeTextFiles(vSQLFile)
    #从文件中获取内容
    vSqlContext = rdd.collect()[0][1]

    #处理换行符
    # rSql = vSqlContext.replace('\n', '')
    # rSql = vSqlContext
    #按分号拆分sql
    sqlList = vSqlContext.split(";",)

    # 处理 Hive SQL兼容性
    hiveSQLCompat = "set spark.sql.hive.convertMetastoreParquet = true"
    spark.sql(hiveSQLCompat)
    hiveSQLCompat = "set spark.sql.ansi.enabled = false"
    spark.sql(hiveSQLCompat)

    spark.sql(f'use {database}')
    #遍历 sqlList 执行, 需要从变量域中获取变量 format_map(vars())，因此sql中定义的变量格式 {parameter}
    for sql in sqlList:
        if sql != '':
            logger.info("execsql:" + sql)
            print("execsql:" + sql)
            spark.sql(sql.format_map(vars())).show()
