import sys
from pyspark.sql.session import SparkSession
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
glueClient = boto3.client('glue')
logger.info('Initialization.')

config = {
    "table_name": "delta_trips_table",
    "database_name": "deltalake",
    "target": "s3://myemr-bucket-01/data/deltalake/delta_trips_table",
    "primary_key": "trip_id",
    "sort_key": "tstamp",
    "commits_to_retain": "4"
}

## Generates Data
def get_json_data(start, count, dest):
    time_stamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = [{"trip_id": i, "tstamp": time_stamp, "route_id": chr(65 + (i % 10)), "destination": dest[i%10]} for i in range(start, start + count)]
    return data

# Creates the Dataframe
def create_json_df(spark, data):
    sc = spark.sparkContext
    return spark.read.json(sc.parallelize(data))

dest = ["Seattle", "New York", "New Jersey", "Los Angeles", "Las Vegas", "Tucson","Washington DC","Philadelphia","Miami","San Francisco"]
df1 = create_json_df(spark, get_json_data(0, 2000000, dest))


additional_options={
    "path": config['target']
}

df1.write.format("delta") \
        .options(**additional_options) \
        .mode('overwrite') \
        .save()

job.commit()