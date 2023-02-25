import sys
from datetime import datetime
import getopt
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *



if __name__ == "__main__":

    # print(len(sys.argv))
    if (len(sys.argv) == 0):
        print("Usage: spark-sql-executor [-f sqlfile,-s s3bucket,-h hivevar]")
        sys.exit(0)
    vSQLFile = ''
    vS3Bucket = ''
    logger = logging.getLogger('py4j')

    opts,args = getopt.getopt(sys.argv[1:],"f:s:h:",["sqlfile=","s3bucket=","hivevar="])
    for opt_name,opt_value in opts:
        if opt_name in ('-f','--sqlfile'):
            vSQLFile = opt_value
            logger.info("SQLFile:" + vSQLFile)
        elif opt_name in ('-s','--s3bucket'):
            vS3Bucket = opt_value
            logger.info("S3Bucket:" + vS3Bucket)
        elif opt_name in ('-h','--hivevar'):
            hivevar = opt_value
            exec(hivevar)
            strhivevar = 'hivevar:' + hivevar
            exec(strhivevar)
            logger.info(strhivevar)
        else:
            logger.info("need parameters [sqlfile,s3bucket,hivevar]")
            exit()
    vWarehouse = "s3://" + vS3Bucket + "/warehouse/"
    logger.info("SQL File: " + vSQLFile)
    logger.info("Warehouse location: " + vWarehouse)

    spark = SparkSession \
        .builder \
        .config("spark.sql.warehouse.dir", vWarehouse) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    rdd = sc.wholeTextFiles(vSQLFile)
    #从文件中获取内容
    vSqlContext = rdd.collect()[0][1]

    #处理换行符
    rSql = vSqlContext.replace('\n', '')

    #按分号拆分sql
    sqlList = rSql.split(";",)

    #遍历 sqlList 执行, 需要从变量域中获取变量 format_map(vars())，因此sql中定义的变量格式 {parameter}
    for sql in sqlList:
        if sql != '':
            logger.info("execsql:" + sql)
            spark.sql(sql.format_map(vars()))
