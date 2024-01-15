import json
import boto3
import os
import datetime


opensearch_user = '<opensearch-user>'
opensearch_password = '<opensearch-password>'
# the OpenSearch Service domain, e.g. search-mydomain.us-west-1.es.amazonaws.com
host = '<opensearch-endpoint>'
index = 'autel-vector-index'

s3 = boto3.client('s3')

JOBNAME = os.environ['glue_jobname']
# Lambda execution starts here
def handler(event, context):
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get, read, and split the file into lines
        glue = boto3.client('glue')

        bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        publish_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print("**** in lambda bucket: " + bucket)
        print("**** in lambda object_key: " + object_key)
        print("**** in lambda publish_date: " + object_key)

        glue.start_job_run(JobName=JOBNAME, Arguments={"--BUCKET": bucket,
                                                       "--OBEJCT_KEY": object_key,
                                                       "--AOS_ENDPOINT": host,
                                                       "--OPENSEARCH_USER": opensearch_user,
                                                       "--OPENSEARCH_PASSWORD": opensearch_password,
                                                       "--INDEX": index})

        return {
            'statusCode': 200,
            'body': json.dumps('Successful ')
    }
