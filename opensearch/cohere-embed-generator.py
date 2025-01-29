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
import json
import logging
import sys
import boto3
from opensearchpy import OpenSearch, helpers, AWSV4SignerAuth, RequestsHttpConnection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# print(sys.argv)
text = sys.argv[1]
# print(text)


client = boto3.client("bedrock-runtime", region_name="us-east-1")

credentials = boto3.Session().get_credentials()

# OpenSearch Cluster
# auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)



headers = {"Content-Type": "application/json"}

def generate_text_embeddings(model_id, body):
    """
    Generate text embedding by using the Cohere Embed model.
    Args:
        model_id (str): The model ID to use.
        body (str) : The reqest body to use.
    Returns:
        dict: The response from the model.
    """

    # logger.info(
    #     "Generating text emdeddings with the Cohere Embed model %s", model_id)

    accept = '*/*'
    content_type = 'application/json'

    bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')

    response = bedrock.invoke_model(
        body=body,
        modelId=model_id,
        accept=accept,
        contentType=content_type
    )

    # logger.info("Successfully generated text with Cohere model %s", model_id)

    return response

def embeding_udf(text):
    """
    User defined function to generate embeddings for text data.
    Args:
        text (str): The text data to generate embeddings for.
    Returns:
        list: The embeddings for the text data.
    """
    # logging.basicConfig(level=logging.INFO,
    #                     format="%(levelname)s: %(message)s")

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

ret = embeding_udf(text)

# print(ret)

# OpenSearch Serverless
# Query
region = 'us-east-1'
service = 'aoss'

auth = AWSV4SignerAuth(credentials, region, service)
host = 'l68ddj9016h1umymg6lf.us-east-1.aoss.amazonaws.com'
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

# query vector
query_vector = ret
query = {
    "size": 5,
    "_source": ["text"],
    "query": {
        "knn": {
            "my_vector": {
                "vector": query_vector,
                "k": 5
            }
        }
    }
}

response = opensearch_client.search(index="vector-index-fp16", body=query)
print(response['took'])
