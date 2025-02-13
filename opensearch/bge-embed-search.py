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
index_name = sys.argv[2]
# print(text)


client = boto3.client("bedrock-runtime", region_name="us-east-1")

credentials = boto3.Session().get_credentials()

# OpenSearch Cluster
# auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)



headers = {"Content-Type": "application/json"}
# Define the function to call the SageMaker endpoint
# BGE Embedding
def call_sagemaker_endpoint(text):
    endpoint_name = "tei-2025-02-02-15-18-25-267"
    region = "us-east-1"
    runtime = boto3.client('sagemaker-runtime', region_name=region)
    # embeddings = None
    # try:
    text = text[:2000]
    response = runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            Body=json.dumps(
                {
                    "inputs": text,
                    "is_query": True,
                    "instruction" :  "Represent this sentence for searching relevant passages:"
                }
            ),
            ContentType="application/json",
        )

    json_str = response['Body'].read().decode('utf8')
    json_obj = json.loads(json_str)
    embeddings = json_obj[0]
    # except Exception as e:
    #     logger.error(e)
    #     logger.error(text)
    return embeddings

ret = call_sagemaker_endpoint(text)

# print(ret)

# OpenSearch Serverless
# Query
region = 'us-east-1'
service = 'aoss'

auth = AWSV4SignerAuth(credentials, region, service)
host = 'ommbieqhyvkib7bv59pa.us-east-1.aoss.amazonaws.com'
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

response = opensearch_client.search(index=index_name, body=query)
print('took:' + str(response['took']))
