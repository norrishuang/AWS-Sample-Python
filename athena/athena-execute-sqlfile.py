import csv
import time
import boto3
import sys
import getopt
import os

from botocore.config import Config


## athena-execute-sqlfile.py
## 通过 Athena 执行 SQL 文件的方法
## 参数描述
#   sqlfiles, sql文件存放的local目录（directory, 例如 ./emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_athena/）
#       sql文件名称（例如 tpcds_2_4_athena.sql）如果一个SQL文件里需要执行多行记录，sql使用; 分割
#   region, 地区（例如 us-east-1）
#   database, 数据库（例如 tpcds）
#   output, 输出结果的目录（directory, 例如 ./）
#   workgroup , 工作组（例如 primary）
#
# 参考
# SQLPATH=./emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_athena/
# DATABASE=s3_exp
# OUTPUT=/home/ec2-user/environment/
# python3 ./emr-on-eks-benchmark/examples/python/athena-execute-sqlfile.py -f $SQLPATH -d $DATABASE -o $OUTPUT
#


SQLFILES = ''
REGION = ''
DATABASE = 'tpcds'
OUTPUT = './'
WORKGROUP = 'primary'
if len(sys.argv) > 1:
    opts, args = getopt.getopt(sys.argv[1:],
                               "f:r:d:o:w:",
                               ["sqlfiles=",
                                "region=",
                                "database=",
                                "output=",
                                "workgroup="])
    for opt_name, opt_value in opts:
        if opt_name in ('-f', '--sqlfiles'):
            SQLFILES = opt_value
            print("SQLFILES:" + SQLFILES)
        elif opt_name in ('-r', '--region'):
            REGION = opt_value
            print("REGION:" + REGION)
        elif opt_name in ('-d', '--database'):
            DATABASE = opt_value
            print("DATABASE:" + DATABASE)
        elif opt_name in ('-o', '--output'):
            OUTPUT = opt_value
            print("OUTPUT:" + OUTPUT)
        elif opt_name in ('-w', '--workgroup'):
            WORKGROUP = opt_value
            print("WORKGROUP:" + WORKGROUP)
        else:
            print("need parameters [sqlfiles, region, database etc.]")
            exit()

else:
    print("Job failed. Please provided params sqlfiles,region .etc ")
    sys.exit(1)

'''
Load SQL files from
/Users/xiohuang/IdeaProjects/emr-on-eks-benchmark/spark-sql-perf/src/main/resources/tpcds_2_4_athena
'''

#write result csv
writeresultfile = "{:s}/result_{:s}.csv".format(OUTPUT, DATABASE)

def load_sql_file(sqlpath):
    ##写结果集的表头 覆盖原来的文件
    with open(writeresultfile, "w", newline='') as csvfile:
        fieldnames = ['SQL', 'DataScannedInBytes', 'TotalExecutionTimeInMillis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    # 每一条SQL执行完成后的等待时间
    waitingTimeSec = 30
    i = 0
    for root, dirs, files in os.walk(sqlpath):
        for file in sorted(files):
            sqlfilepath = os.path.join(root, file)
            sqlfile = open(sqlfilepath, encoding='utf-8')
            sqltext = sqlfile.read()
            sqlfile.close()

            print('exec sql:' + sqlfilepath)
            #按分号拆分sql
            sqlList = sqltext.split(";",)
            totalNum = len(sqlList)
            for sql in sqlList:
                if sql != '':
                    print("execsql:" + sql)
                    executeSQL(file, sql)
                    # wait
                    print("Execute SQL Complated, Wait.")
                    time.sleep(waitingTimeSec)
                    i = i + 1
                    print("process:[{:d}/{:d}]".format(i, totalNum))
            time.sleep(10)


def executeSQL(filename, sqltext):

    my_config = Config(
        region_name = 'us-east-1',
        signature_version = 'v4',
        retries = {
            'max_attempts': 10,
            'mode': 'standard'
        }
    )
    client = boto3.client('athena', config=my_config)
    responseQuery = client.start_query_execution(
        QueryString=sqltext,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': 's3://aws-athena-query-results-us-east-1-812046859005/',
        },
        WorkGroup=WORKGROUP
    )

    DataScannedInBytes = 0.0
    TotalExecutionTimeInMillis = 0.0
    while True:
        try:
            responseStatus = client.get_query_execution(
                QueryExecutionId=responseQuery['QueryExecutionId']
            )
            # get runing status from query execution of athena
            query_results = responseStatus['QueryExecution']['Status']['State']
            # get a status from query execution of athena
            if query_results == 'SUCCEEDED':
                query_results = client.get_query_execution(
                    QueryExecutionId=responseQuery['QueryExecutionId']
                )
                DataScannedInBytes = round(float(int(query_results['QueryExecution']['Statistics']['DataScannedInBytes'])/1024/1024/1024),3)
                TotalExecutionTimeInMillis = float(int(query_results['QueryExecution']['Statistics']['TotalExecutionTimeInMillis'])/1000)
                break
            elif query_results == 'FAILED':
                DataScannedInBytes = -999
                TotalExecutionTimeInMillis = -999
                print('Execute Failure.')
                print(query_results['QueryExecution']['Status']['AthenaError'])
                break
            time.sleep(3)

        except Exception as err:
            # responseerr = err.response
            # code = responseerr['Error']['Code']
            # message = responseerr['Error']['Message']
            DataScannedInBytes = -999
            TotalExecutionTimeInMillis = -999
            print(err)
            break

    with open(writeresultfile, "a+", newline='') as csvfile:
        fieldnames = ['SQL', 'DataScannedInBytes', 'TotalExecutionTimeInMillis']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        #
        writer.writerow({'SQL': filename,
                         'DataScannedInBytes': DataScannedInBytes,
                         'TotalExecutionTimeInMillis': TotalExecutionTimeInMillis})


load_sql_file(SQLFILES)
