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
import getopt

# Spark job to ingest text embeddings into OpenSearch
# This script reads text data from an S3 bucket, generates embeddings using the Cohere Embed model, and indexes the embeddings into OpenSearch
# The script uses the OpenSearch Python client to interact with the OpenSearch cluster
# The script uses the boto3 library to interact with the Cohere API
# The script uses the Spark DataFrame API to read data from S3 and generate embeddings
# The script uses the OpenSearch Python client to index the embeddings into OpenSearch

from pyspark.sql import SparkSession
# from awsglue.utils import getResolvedOptions
from opensearchpy import OpenSearch, helpers, AWSV4SignerAuth, RequestsHttpConnection
import json
import sys
import logging
import boto3
import pyspark.pandas as ps

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)



print(sys.argv)
if len(sys.argv) == 0:
    print("Usage: embeding job [-u opensearch_user,-p opensearch_password,-e aos_endpoint,-i index,-d input_path]")
    sys.exit(0)

    # para
OPENSEARCH_USER = ""
OPENSEARCH_PASSWORD = ""
AOS_ENDPOINT = ""
INSERT_INDEX = ""
S3_INPUT_PATH = ""
JOB_NAME = ""
AOS_TYPE = "cluster"

opts,args = getopt.getopt(sys.argv[1:],"t:u:p:e:i:d:j:", \
                              ["aos_type=","opensearch_user=","opensearch_password=","aos_endpoint=","index=","input_path=","jobname="])
for opt_name,opt_value in opts:
    if opt_name in ('-u','--opensearch_user'):
        OPENSEARCH_USER = opt_value
        logger.info("OPENSEARCH_USER:" + OPENSEARCH_USER)
    elif opt_name in ('-p','--opensearch_password'):
        OPENSEARCH_PASSWORD = opt_value
        logger.info("OPENSEARCH_PASSWORD:" + OPENSEARCH_PASSWORD)
    elif opt_name in ('-e','--aos_endpoint'):
        AOS_ENDPOINT = opt_value
        logger.info("AOS_ENDPOINT:" + AOS_ENDPOINT)
    elif opt_name in ('-i','--index'):
        INSERT_INDEX = opt_value
        logger.info("INSERT_INDEX:" + INSERT_INDEX)
    elif opt_name in ('-d','--input_path'):
        S3_INPUT_PATH = opt_value
        logger.info("S3_INPUT_PATH:" + S3_INPUT_PATH)
    elif opt_name in ('-j','--jobname'):
        JOB_NAME = opt_value
        logger.info("S3_INPUT_PATH:" + JOB_NAME)
    elif opt_name in ('-t','--aos_type'):
        AOS_TYPE = opt_value
        logger.info("AOS_TYPE:" + AOS_TYPE)
    else:
        logger.info("need parameters [jobnname,aos_endpoint,input_path,index] input:%s",opt_name)
        exit()

# args = getResolvedOptions(sys.argv, ['S3_PATH','AOS_ENDPOINT','OPENSEARCH_USER','OPENSEARCH_PASSWORD','INDEX'])

client = boto3.client("bedrock-runtime", region_name="us-east-1")

credentials = boto3.Session().get_credentials()

# OpenSearch Cluster
# auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)




# the OpenSearch Service domain, e.g. search-mydomain.us-west-1.es.amazonaws.com
host = AOS_ENDPOINT
index_insert = INSERT_INDEX
s3_path = S3_INPUT_PATH

opensearch_client = None

headers = {"Content-Type": "application/json"}

# Initialize Spark session
spark = SparkSession.builder.appName(JOB_NAME).getOrCreate()

# Read data from a file
df = spark.read.text(s3_path)


def generate_text_embeddings(model_id, body):
    """
    Generate text embedding by using the Cohere Embed model.
    Args:
        model_id (str): The model ID to use.
        body (str) : The reqest body to use.
    Returns:
        dict: The response from the model.
    """

    logger.info(
        "Generating text emdeddings with the Cohere Embed model %s", model_id)

    accept = '*/*'
    content_type = 'application/json'

    bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

    response = bedrock.invoke_model(
        body=body,
        modelId=model_id,
        accept=accept,
        contentType=content_type
    )

    logger.info("Successfully generated text with Cohere model %s", model_id)

    return response

def embeding_udf(text):
    """
    User defined function to generate embeddings for text data.
    Args:
        text (str): The text data to generate embeddings for.
    Returns:
        list: The embeddings for the text data.
    """
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s: %(message)s")

    model_id = 'cohere.embed-english-v3'
    input_type = "search_document"
    embedding_types = ["float"]
    text = text[:1500]
    body = json.dumps({
        "texts": [text],
        "input_type": input_type,
        "embedding_types": embedding_types}
    )
    response = generate_text_embeddings(model_id=model_id,
                                        body=body)

    response_body = json.loads(response.get('body').read())

    # logger.info(f"ID: {response_body.get('id')}")
    # logger.info(f"Response type: {response_body.get('response_type')}")
    #
    # logger.info("Embeddings")
    # for i, embedding in enumerate(response_body.get('embeddings')):
    #     logger.info(f"\tEmbedding {i}")
    #     logger.info(*embedding)

    return response_body.get('embeddings').get('float')[0]

df_partition = df.filter('value is not null and len(value)<=2000').limit(1000000).repartition(8)

# TotalCount = df_partition.count()
#
# logger.info("TotalCount:" + str(TotalCount))

# process_data_udf = pandas_udf(embeding_udf, returnType=StringType(), functionType=PandasUDFType.SCALAR)

embedding_udf = spark.udf.register("embeding_udf", embeding_udf)

df_embedding = df_partition.withColumn("embedding", embedding_udf(df_partition["value"]))
# df = df.limit(10)
# Convert DataFrame to Pandas for easier handling with OpenSearch

# pandas_df = df_embedding.toPandas().repartition(8)
pandas_df = ps.DataFrame(df_embedding)

if AOS_TYPE=="cluster":
    # Initialize OpenSearch client
    # OpenSearch Cluster
    auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
    opensearch_client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_compress=True,  # enables gzip compression for request bodies
        use_ssl=True,
        verify_certs=False,
        http_auth=auth,
        timeout=120
    )

else:
    # OpenSearch Serverless
    region = 'us-east-1'
    service = 'aoss'
    auth = AWSV4SignerAuth(credentials, region, service)
    opensearch_client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth = auth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection,
        pool_maxsize = 20
    )

bulk_docs = []
counts = 0
# Index the embeddings into OpenSearch
for index, row in pandas_df.iterrows():
    document = {
        "_index": index_insert,
        "text": row["value"],
        "my_vector": json.loads(row["embedding"])
    }
    if counts != 0:
        bulk_docs.append(document)
        # r = requests.post(url, auth=awsauth, json=document, headers=headers)
        # print("request:" + r.text)

    counts = counts + 1
    # per 100 records bulk insert
    if counts % 100 == 0:
        # print("input: %d" % counts)
        # client.bulk(bulk_docs)
        try:
            response = helpers.bulk(opensearch_client, bulk_docs, max_retries=3)
            logger.info(
                "Bulk insert %d records into OpenSearch", counts)
        except Exception as e:
            logger.error(
                "Bulk insert records into OpenSearch failed")
            logger.error(e)
        # print(response)
        bulk_docs = []
        counts = 0
        continue

response = helpers.bulk(opensearch_client, bulk_docs, max_retries=3)
spark.stop()