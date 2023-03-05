# -*- coding: utf-8 -*-
# 使用 spark 实现数据同步
# 支持源-----
# 1.MYSQL
# 支持目标----
# 1.S3
import time
import sys
import getopt
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

# 远程调试需要添加环境变量进去
logger = logging.getLogger('py4j')

if __name__ == '__main__':

    if (len(sys.argv) == 0):
        print("Usage: spark-etl-job [-j jdbcdriver,-l jdbcurl,-u user,-p password,-d database,-t tablename,-s s3path]")
        sys.exit(0)

    #default
    vWriteMode = "append"
    vJDBCDriver = ""
    vJDBCUrl = ""
    vUser = ""
    # 传参
    opts,args = getopt.getopt(sys.argv[1:],"j:l:u:p:d:t:s:ao",\
                              ["jdbcdriver=","jdbcurl=","user=","password=","database=","tablename=","s3path=","append","overwrite"])
    for opt_name,opt_value in opts:
        if opt_name in ('-j','--jdbcdriver'):
            vJDBCDriver = opt_value
            logger.info("JDBCDriver:" + vJDBCDriver)
        elif opt_name in ('-l','--jdbcurl'):
            vJDBCUrl = opt_value
            logger.info("JDBCURL:" + vJDBCUrl)
        elif opt_name in ('-u','--user'):
            vUser = opt_value
        elif opt_name in ('-p','--password'):
            vPassword = opt_value
        elif opt_name in ('-d','--database'):
            vDatabase = opt_value
        elif opt_name in ('-t','--tablename'):
            vTablename = opt_value
        elif opt_name in ('-s','--s3path'):
            vS3Path = opt_value
        elif opt_name in ('-a','--append'):
            vWriteMode = 'append'
        elif opt_name in ('-o','--overwrite'):
            vWriteMode = 'append'
        else:
            logger.info("need parameters [jdbcdriver,jdbcurl,user,password,database,tablename,s3path]")
            exit()

    if vJDBCDriver == "" :
        logger.info("need parameters [JDBCDriver]")
        exit()

    if vJDBCUrl == "" :
        logger.info("need parameters [JDBCUrl]")
        exit()

    # 构建SparkSession执行环境入口对象
    spark = SparkSession \
        .builder \
        .appName("SparkETL") \
        .enableHiveSupport() \
        .getOrCreate()
    # config("spark.driver.extraClassPath", "/export/project/02_SQL/database/sqljdbc_10.2/chs/mssql-jdbc-10.2.1.jre8.jar").\


    # 数据库读出
    dfTable = spark.read.format("jdbc"). \
        option("driver", vJDBCDriver). \
        option("url", vJDBCUrl). \
        option('dbtable', vTablename). \
        option("user", vUser). \
        option("password", vPassword). \
        option("encoding", "utf8"). \
        load()

    # write s3
    dfTable.write.mode(vWriteMode).option("partitionOverwriteMode", "dynamic").parquet(vS3Path)


