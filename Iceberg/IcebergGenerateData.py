import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from awsglue import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger=glueContext.get_logger()
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", "s3://myemr-bucket-01/data/iceberg-folder/")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager")
spark.conf.set("spark.sql.catalog.glue_catalog.lock.table", "datacoding_iceberg_lock_table")


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
dataFrame = create_json_df(spark, get_json_data(0, 2000000, mor_dest))

dataFrameDF = DynamicFrame.fromDF(dataFrame, glueContext, "from_data_frame")
dataFrameDF = dataFrameDF.apply_mapping(
    [
        ("trip_Id", "String", "trip_Id", "bigint"),
        ("tstamp", "String", "tstamp", "String"),
        ("route_id", "String", "route_id", "String"),
        ("destination", "String", "destination", "String")
    ]
)

dataFrame = dataFrameDF.toDF()
dataFrame.createOrReplaceTempView("tmp_trip_table")
query = f"""INSERT INTO glue_catalog.iceberg_db.iceberg_trips_table  SELECT * FROM tmp_trip_table"""

# glueContext.write_data_frame.from_catalog(
#     frame=dataFrame,
#     database="iceberg_db",
#     table_name="iceberg_trips_table"
# )
# 把 dataframe 转换成字符串，在logger中输出
def getShowString(df, n=10, truncate=True, vertical=False):
    if isinstance(truncate, bool) and truncate:
        return df._jdf.showString(n, 10, vertical)
    else:
        return df._jdf.showString(n, int(truncate), vertical)

# spark.sql(query)

logger.info("############  dataFrame  ############### \r\n" + getShowString(dataFrame,truncate = False))

dataFrame.write.format("iceberg").mode("append").save("glue_catalog.iceberg_db.iceberg_trips_table")


job.commit()
