#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


import sys
import boto3
import json
import logging
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkConf
from pyspark.sql import DataFrame, Row
from pyspark.sql import SparkSession
from awsglue import DynamicFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType,MapType,LongType
from pyspark.sql.functions import from_json,col,to_json,json_tuple
from pyspark.sql.functions import current_timestamp, unix_timestamp
from pyspark.sql.functions import lit
from pyspark.sql.types import TimestampType

def get_secret():

    secret_name = "prod/redshift"
    region_name = "eu-central-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

secret_dict = json.loads(get_secret())
print("---------------start")
params = [
    'JOB_NAME',
    'TempDir',
    'kafka_broker',
    'topic',
    'startingOffsets',
    'checkpoint_interval',
    'checkpoint_location',
    'aws_region'
]
args = getResolvedOptions(sys.argv, params)
conf = SparkConf()

sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
#logger = glueContext.get_logger()
logger = logging.getLogger("mylogger")
logger.setLevel(logging.INFO)
spark = glueContext.spark_session
job_name = args['JOB_NAME']
kafka_broker = args['kafka_broker']
topic = args['topic']
startingOffsets = args['startingOffsets']
checkpoint_interval = args['checkpoint_interval']
checkpoint_location = args['checkpoint_location']
aws_region = args['aws_region']


maxerror = 0
redshift_host = secret_dict['host']
redshift_port = secret_dict['port']
redshift_username = secret_dict['username']
redshift_password = secret_dict['password']
redshift_database = secret_dict['dbname']
kafka_username = secret_dict["kafka_username"]
kafka_password = secret_dict["kafka_password"]
redshift_schema = "public"
redshift_table = "base_table_202404"
redshift_tmpdir = "s3://cloud-data-glue-temp/temp/"
tempformat = "CSV"
redshift_iam_role = "arn:aws:iam::661737937633:role/RedshiftAccessMSKRole"

reader = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .option("maxOffsetsPerTrigger", "1000000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .option("failOnDataLoss", "false")

if startingOffsets == "earliest" or startingOffsets == "latest":
    reader.option("startingOffsets", startingOffsets)
else:
    reader.option("startingTimestamp", startingOffsets)


kafka_data = reader.load()
# df = kafka_data.selectExpr("CAST(value AS STRING)")
df = kafka_data.selectExpr("CAST(value AS STRING)","CAST(timestamp AS BIGINT)")

def process_batch(data_frame, batchId):
    dfc = data_frame.cache()
    print(job_name + " - my_log - process batch id: " + str(batchId) + " record number: " + str(dfc.count()))
    if not data_frame.rdd.isEmpty():
        json_schema = spark.read.json(dfc.rdd.map(lambda p: str(p["value"]))).schema
        df_rename = dfc.withColumnRenamed("value", "kafka_data")
        # df_with_timestamp = df_rename.withColumn("redshift_time", unix_timestamp(lit(current_timestamp().cast(TimestampType()))))
        df_with_timestamp = df_rename.withColumnRenamed("timestamp", "kafka_time")
        super_column_list=["kafka_data"]
        fields = []
        for field in df_with_timestamp.schema.fields:
            if field.name in super_column_list:
                # metadata={'redshift_type','super'}
                sf = StructField(field.name, field.dataType, field.nullable,
                                 metadata={"super": True, "redshift_type": "super"})
            else:
                sf = StructField(field.name, field.dataType, field.nullable)
            fields.append(sf)
        schema_with_super_metadata = StructType(fields)
        df_super = spark.createDataFrame(df_with_timestamp.rdd, schema_with_super_metadata)


        #print(job_name + " - my_log - convert csdf schema: " + df_super._jdf.schema().treeString())
        #print(job_name + " - my_log - convert csdf: " + df_super._jdf.showString(5, 20, False))
        df_super.write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", "jdbc:redshift://{0}:{1}/{2}".format(redshift_host, redshift_port, redshift_database)) \
            .option("dbtable", "{0}.{1}".format(redshift_schema,redshift_table)) \
            .option("user", redshift_username) \
            .option("password", redshift_password) \
            .option("tempdir", redshift_tmpdir) \
            .option("tempformat", tempformat) \
            .option("extracopyoptions", "TRUNCATECOLUMNS region '{0}' maxerror {1} dateformat 'auto' timeformat 'auto'".format(aws_region, maxerror)) \
            .option("aws_iam_role",redshift_iam_role).mode("append").save()
    dfc.unpersist()
    #print(job_name + " - my_log - finish batch id: " + str(batchId))


save_to_redshift = df \
    .writeStream \
    .outputMode("append") \
    .trigger(processingTime="{0} seconds".format(checkpoint_interval)) \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_location) \
    .start()

save_to_redshift.awaitTermination()