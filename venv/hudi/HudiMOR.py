import sys

from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame
import boto3


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
logger.info('Initialization.')

# General Constants
HUDI_FORMAT = "org.apache.hudi"

STORAGE_TYPE_OPT_KEY="hoodie.datasource.write.storage.type"
COMPACTION_INLINE_OPT_KEY="hoodie.compact.inline"
COMPACTION_MAX_DELTA_COMMITS_OPT_KEY="hoodie.compact.inline.max.delta.commits"

def get_json_data(start, count, dest):
    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = [{"trip_id": i, "tstamp": time_stamp, "route_id": chr(65 + (i % 10)), "destination": dest[i%10]} for i in range(start, start + count)]
    return data

# Creates the Dataframe
def create_json_df(spark, data):
    sc = spark.sparkContext
    return spark.read.json(sc.parallelize(data))

#Let's generate 2M records to load into our Data Lake:
mor_dest = ["Seattle", "New York", "New Jersey", "Los Angeles", "Las Vegas", "Tucson","Washington DC","Philadelphia","Miami","San Francisco"]
df2 = create_json_df(spark, get_json_data(0, 2000000, mor_dest))

config = {
    "table_name": "hudi_trips_table_mor",
    "database_name": "hudi",
    "target": "s3://myemr-bucket-01/data/hudi/hudi_trips_table_mor/",
    "primary_key": "trip_id",
    "sort_key": "tstamp",
    "commits_to_retain": "3",
}

additional_options={
    "hoodie.table.name": config['table_name'],
    "hoodie.datasource.write.storage.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": config["primary_key"],
    "hoodie.datasource.write.precombine.field": config["sort_key"],
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.datasource.hive_sync.database": config['database_name'],
    "hoodie.datasource.hive_sync.table": config['table_name'],
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "path": config['target']
}

# df2.write.format(HUDI_FORMAT).options(**additional_options).mode("Overwrite").save()

# glueContext.write_data_frame.from_catalog(frame = DynamicFrame.fromDF(df2, glueContext, "outputDF"),
#                                              database = config['database_name'],
#                                              table_name = config['table_name'],
#                                              connection_options = additional_options)
glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(df2, glueContext, "outputDF"),
                                             connection_type = "s3",
                                             connection_options = additional_options)
job.commit()