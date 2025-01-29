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

# Glue job to ingest text embeddings into OpenSearch
# This script reads text data from an S3 bucket, generates embeddings using the Cohere Embed model, and indexes the embeddings into OpenSearch
# The script uses the OpenSearch Python client to interact with the OpenSearch cluster
# The script uses the boto3 library to interact with the Cohere API
# The script uses the Spark DataFrame API to read data from S3 and generate embeddings
# The script uses the OpenSearch Python client to index the embeddings into OpenSearch

from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import requests
from opensearchpy import OpenSearch, helpers, AWSV4SignerAuth, RequestsHttpConnection
import json
import sys
import logging
import boto3
from pyspark.sql.pandas.functions import pandas_udf, PandasUDFType


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, ['S3_PATH','AOS_ENDPOINT','OPENSEARCH_USER','OPENSEARCH_PASSWORD','INDEX'])

client = boto3.client("bedrock-runtime", region_name="us-east-1")

credentials = boto3.Session().get_credentials()

# OpenSearch Cluster
auth = (args['OPENSEARCH_USER'], args['OPENSEARCH_PASSWORD'])

# OpenSearch Serverless
region = 'us-east-1'
service = 'aoss'
auth = AWSV4SignerAuth(credentials, region, service)


# the OpenSearch Service domain, e.g. search-mydomain.us-west-1.es.amazonaws.com
host = args['AOS_ENDPOINT']
index_insert = args['INDEX']

s3_path = args['S3_PATH']

headers = {"Content-Type": "application/json"}

# Initialize Spark session
spark = SparkSession.builder.appName("EmbeddingPipeline").getOrCreate()

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

    bedrock = boto3.client(service_name='bedrock-runtime')

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

df_partition = df.repartition(8).filter('value is not null and len(value)<=2000')
# Apply the embedding function to the DataFrame
# embedding_udf = spark.udf.register("embeding_udf", embeding_udf)
# df = df.limit(1000)
# df_embed = df_partition.withColumn("embedding", df_partition["value"])



# Convert DataFrame to Pandas for easier handling with OpenSearch
pandas_df = df_partition.toPandas()
pandas_df_embedding = pandas_df.withColumn("embedding", embeding_udf(df_partition["value"]))


# Initialize OpenSearch client
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,  # enables gzip compression for request bodies
    use_ssl=True,
    verify_certs=False,
    http_auth=auth,
    timeout=120
)

client_serverless = OpenSearch(
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
for index, row in pandas_df_embedding.iterrows():
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
    # per 50 records bulk insert
    if counts % 50 == 0:
        # print("input: %d" % counts)
        # client.bulk(bulk_docs)
        response = helpers.bulk(client, bulk_docs, max_retries=3)
        # print(response)
        bulk_docs = []
        counts = 0
        continue

response = helpers.bulk(client, bulk_docs, max_retries=3)
spark.stop()