#!/usr/bin/env python3
"""
OpenSearch Vector Benchmark Data Generator

This script generates random simulated data and imports it into OpenSearch.
The data structure includes content (string), platform (string), date (date),
tag (list), and content_vector (1536-dimensional array).
"""

import argparse
import datetime
import json
import numpy as np
import random
import string
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
from faker import Faker

# Initialize Faker for generating random text
fake = Faker()

# Default OpenSearch connection settings
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'admin'
USE_SSL = True  # Amazon OpenSearch Service requires HTTPS

# Default index settings
INDEX_NAME = 'vector_benchmark'
VECTOR_DIMENSION = 1536

def create_opensearch_client(host, port, username, password):
    """Create and return an OpenSearch client for Amazon OpenSearch Service."""
    client = OpenSearch(
        hosts=[{'host': host, 'port': int(port)}],
        http_auth=(username, password),
        use_ssl=USE_SSL,
        verify_certs=False,  # Set to False for development without certificates
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=60  # Increase timeout for better reliability
    )
    return client

def create_index(client, index_name):
    """Create an OpenSearch index with vector field configuration."""
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            }
        },
        "mappings": {
            "properties": {
                "content": {"type": "text"},
                "platform": {"type": "keyword"},
                "date": {"type": "date"},
                "tag": {"type": "keyword"},
                "content_vector": {
                    "type": "knn_vector",
                    "dimension": VECTOR_DIMENSION,
                    "space_type": "innerproduct",
                    "method": {
                        "name": "hnsw",
                        "engine": "faiss",
                        "parameters": {
                            "encoder": {
                                "name": "sq",
                                "parameters": {
                                    "type": "fp16"
                                }
                            },
                            "ef_construction": 32,
                            "m": 8
                        }
                    }
                }
            }
        }
    }
    
    # Check if index exists and delete if it does
    if client.indices.exists(index=index_name):
        print(f"Index {index_name} already exists. Deleting...")
        client.indices.delete(index=index_name)
    
    # Create the index
    response = client.indices.create(index=index_name, body=index_body)
    print(f"Index created: {response}")
    return response

def generate_random_vector(dimension=VECTOR_DIMENSION):
    """Generate a random vector with the specified dimension."""
    return np.random.uniform(-1, 1, dimension).tolist()

def generate_random_tags(max_tags=5):
    """Generate a random list of tags."""
    tags = ["technology", "science", "art", "business", "health", 
            "education", "entertainment", "sports", "politics", "travel"]
    num_tags = random.randint(1, min(max_tags, len(tags)))
    return random.sample(tags, num_tags)

def generate_random_platform():
    """Generate a random platform name."""
    platforms = ["web", "mobile", "desktop", "api", "iot", "cloud"]
    return random.choice(platforms)

def generate_random_date(start_date=datetime.date(2020, 1, 1)):
    """Generate a random date from start_date to today."""
    end_date = datetime.date.today()
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    random_date = start_date + datetime.timedelta(days=random_number_of_days)
    return random_date.isoformat()

def generate_random_document():
    """Generate a random document with the required fields."""
    return {
        "content": fake.paragraph(nb_sentences=random.randint(3, 8)),
        "platform": generate_random_platform(),
        "date": generate_random_date(),
        "tag": generate_random_tags(),
        "content_vector": generate_random_vector()
    }

def generate_bulk_documents(num_docs, index_name):
    """Generate multiple documents for bulk indexing."""
    for i in range(num_docs):
        doc = generate_random_document()
        yield {
            "_index": index_name,
            "_source": doc
        }
        if (i + 1) % 1000 == 0:
            print(f"Generated {i + 1} documents")

def bulk_index_documents(client, num_docs, index_name, batch_size=100):
    """Bulk index the generated documents into OpenSearch."""
    print(f"Indexing {num_docs} documents...")
    
    # 减小批量大小以避免请求过大
    total_success = 0
    total_failed = 0
    
    for i in range(0, num_docs, batch_size):
        # 计算当前批次的实际大小
        current_batch_size = min(batch_size, num_docs - i)
        print(f"Processing batch {i//batch_size + 1}: documents {i+1} to {i+current_batch_size}")
        
        # 为当前批次生成文档
        batch_docs = []
        for j in range(current_batch_size):
            doc = generate_random_document()
            batch_docs.append({
                "_index": index_name,
                "_source": doc
            })
        
        # 批量索引当前批次
        try:
            success, failed = helpers.bulk(
                client,
                batch_docs,
                stats_only=True,
                request_timeout=60  # 增加超时时间
            )
            total_success += success
            total_failed += failed
            print(f"Batch {i//batch_size + 1} completed: {success} succeeded, {failed} failed")
        except Exception as e:
            print(f"Error in batch {i//batch_size + 1}: {e}")
            total_failed += current_batch_size
    
    print(f"Indexing complete. Total: {total_success} succeeded, {total_failed} failed")
    return total_success, total_failed

def main():
    """Main function to parse arguments and execute the script."""
    parser = argparse.ArgumentParser(description='Generate and index random data into OpenSearch')
    parser.add_argument('--num_docs', type=int, default=1000,
                        help='Number of documents to generate and index')
    parser.add_argument('--host', type=str, default=OPENSEARCH_HOST,
                        help='OpenSearch host (for Amazon OpenSearch Service, use the full domain endpoint)')
    parser.add_argument('--port', type=int, default=OPENSEARCH_PORT,
                        help='OpenSearch port (usually 443 for Amazon OpenSearch Service)')
    parser.add_argument('--user', type=str, default=OPENSEARCH_USER,
                        help='OpenSearch username')
    parser.add_argument('--password', type=str, default=OPENSEARCH_PASSWORD,
                        help='OpenSearch password')
    parser.add_argument('--index', type=str, default=INDEX_NAME,
                        help='OpenSearch index name')
    parser.add_argument('--aws-auth', action='store_true',
                        help='Use AWS IAM authentication instead of basic auth')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='AWS region for OpenSearch service (required for AWS auth)')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Number of documents to index in each batch (default: 100)')
    
    args = parser.parse_args()
    
    try:
        # Create OpenSearch client
        print(f"Connecting to OpenSearch at {args.host}:{args.port}...")
        
        if args.aws_auth:
            try:
                from requests_aws4auth import AWS4Auth
                import boto3
                
                if not args.region:
                    print("Error: --region is required when using --aws-auth")
                    return
                
                # Get AWS credentials
                session = boto3.Session()
                credentials = session.get_credentials()
                aws_auth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    args.region,
                    'es',
                    session_token=credentials.token
                )
                
                # Create client with AWS auth
                client = OpenSearch(
                    hosts=[{'host': args.host, 'port': int(args.port)}],
                    http_auth=aws_auth,
                    use_ssl=True,
                    verify_certs=False,
                    connection_class=RequestsHttpConnection,
                    timeout=60
                )
            except ImportError:
                print("Error: AWS authentication requires 'requests_aws4auth' package.")
                print("Please install it with: pip install requests_aws4auth")
                return
        else:
            # Create client with basic auth
            client = create_opensearch_client(args.host, args.port, args.user, args.password)
        
        # Check if OpenSearch is running
        try:
            info = client.info()
            print(f"Successfully connected to OpenSearch. Version: {info.get('version', {}).get('number', 'unknown')}")
        except Exception as e:
            print(f"Connection error: {e}")
            print("\nTroubleshooting tips:")
            print("1. For Amazon OpenSearch Service, make sure you're using the full domain endpoint as host")
            print("2. The port should typically be 443 for Amazon OpenSearch Service")
            print("3. Verify your credentials or IAM permissions")
            print("4. Check if your IP is allowed in the access policy")
            return
        
        # Create index
        create_index(client, args.index)
        
        # Generate and index documents
        bulk_index_documents(client, args.num_docs, args.index, args.batch_size)
        
        print(f"Completed indexing {args.num_docs} documents to {args.index}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
