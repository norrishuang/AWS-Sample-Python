import json
import boto3
import os
import logging
from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection, helpers

logger = logging.getLogger(__name__)

opensearch_user = '<opensearch-user>'
opensearch_password = '<opensearch-password>'
# the OpenSearch Service domain, e.g. search-mydomain.us-west-1.es.amazonaws.com
host = '<opensearch-endpoint>'
index = 'autel-vector-index'

s3 = boto3.client('s3')

# OpenSearch Serverless
region = 'us-east-1'
service = 'aoss'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)


def generate_text_embeddings(model_id, body):
    """
    Generate text embedding by using the Cohere Embed model.
    Args:
        model_id (str): The model ID to use.
        body (str) : The reqest body to use.
    Returns:
        dict: The response from the model.
    """

    # logger.info("Generating text emdeddings with the Cohere Embed model %s", model_id)

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

# Cohere Embedding
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
        obj = s3.get_object(Bucket=bucket, Key=object_key)
        body = obj['Body'].read()
        lines = body.splitlines()
        bulk_docs = []
        index_insert = 'vector-index'
        for line in lines:
            if line == b'':
                continue
            embedding_value = embeding_udf(line.decode('utf-8'))
            document = {
                "_index": index_insert,
                "text": line,
                "my_vector": json.loads(embedding_value)
            }
            bulk_docs.append(document)
            # r = requests.post(url, auth=awsauth, json=document, headers=headers)
            # print("request:" + r.text)
            counts = counts + 1
            # per 100 records bulk insert
            if counts % 20 == 0:
                # print("input: %d" % counts)
                # client.bulk(bulk_docs)
                try:
                    response = helpers.bulk(opensearch_client, bulk_docs, max_retries=5)
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
