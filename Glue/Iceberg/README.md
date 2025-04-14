# AWS Glue Streaming with Apache Iceberg

This project demonstrates how to use AWS Glue to stream data from Apache Kafka to Apache Iceberg tables stored in Amazon S3.

## Files

- `Lotus-GlueStreaming.py`: Original script that streams data from Kafka to Amazon Redshift
- `Lotus-GlueStreaming-Iceberg.py`: Modified script that streams data from Kafka to Apache Iceberg tables in S3

## Prerequisites

1. AWS Glue job with appropriate IAM permissions
2. Apache Kafka cluster with authentication configured
3. AWS Glue Data Catalog with database and table defined for Iceberg
4. S3 bucket for storing Iceberg data
5. AWS Secrets Manager secret containing Kafka credentials

## Running the Glue Job

To run the Glue job, you need to provide the following parameters:

```
--JOB_NAME=KafkaToIcebergStreaming
--TempDir=s3://your-bucket/temp/
--kafka_broker=your-kafka-broker:9092
--topic=your-kafka-topic
--startingOffsets=latest
--checkpoint_interval=60
--checkpoint_location=s3://your-bucket/checkpoint/
--aws_region=eu-central-1
--catalog=glue_catalog
--database_name=your_database
--table_name=your_table
--iceberg_s3_path=s3://your-bucket/iceberg/
```

## Iceberg Table Creation

Before running the job, create an Iceberg table in the AWS Glue Data Catalog:

```sql
CREATE TABLE your_database.your_table (
  kafka_data STRING,
  kafka_time BIGINT
)
PARTITIONED BY (kafka_time)
STORED BY ICEBERG
LOCATION 's3://your-bucket/iceberg/your_database/your_table';
```

## Notes

- The script uses AWS Secrets Manager to retrieve Kafka credentials
- Ensure your Glue job has the appropriate Spark configuration for Iceberg
- Adjust the schema in the `process_batch` function to match your data structure
