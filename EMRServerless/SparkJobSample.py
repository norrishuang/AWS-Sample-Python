import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 2):
        print("Usage: spark-sql-executor [s3://<bucket>/sample.sql] [s3 bucket name]")
        sys.exit(0)
    vSQLFile = sys.argv[0]
    vS3Bucket = sys.argv[1]
    vWarehouse = "s3://" + sys.argv[1] + "/warehouse/"
    print("SQL File: " + vSQLFile)
    print("Warehouse location: " + vWarehouse)

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

    #遍历 sqlList 执行
    for sql in sqlList:
        if sql != '':
            spark.sql(sql)
