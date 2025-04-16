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
from opensearchpy import OpenSearch, helpers
from faker import Faker

# Initialize Faker for generating random text
fake = Faker()

# Default OpenSearch connection settings
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'admin'
USE_SSL = False  # Set to True for production with proper certificates

# Default index settings
INDEX_NAME = 'vector_benchmark'
VECTOR_DIMENSION = 1536

def create_opensearch_client(host, port, username, password):
    """Create and return an OpenSearch client."""
    client = OpenSearch(
        hosts=[{'host': host, 'port': port}],
        http_auth=(username, password),
        use_ssl=USE_SSL,
        verify_certs=False,  # Set to True in production with proper certificates
        ssl_show_warn=False
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
                    "method": {
                        "name": "hnsw",
                        "engine": "faiss",
                        "space_type": "l2",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 16
                        }
                    },
                    "element_type": "float16"
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

def bulk_index_documents(client, num_docs, index_name):
    """Bulk index the generated documents into OpenSearch."""
    print(f"Indexing {num_docs} documents...")
    success, failed = helpers.bulk(
        client,
        generate_bulk_documents(num_docs, index_name),
        stats_only=True
    )
    print(f"Successfully indexed {success} documents. Failed: {failed}")
    return success, failed

def main():
    """Main function to parse arguments and execute the script."""
    parser = argparse.ArgumentParser(description='Generate and index random data into OpenSearch')
    parser.add_argument('--num_docs', type=int, default=1000,
                        help='Number of documents to generate and index')
    parser.add_argument('--host', type=str, default=OPENSEARCH_HOST,
                        help='OpenSearch host')
    parser.add_argument('--port', type=int, default=OPENSEARCH_PORT,
                        help='OpenSearch port')
    parser.add_argument('--user', type=str, default=OPENSEARCH_USER,
                        help='OpenSearch username')
    parser.add_argument('--password', type=str, default=OPENSEARCH_PASSWORD,
                        help='OpenSearch password')
    parser.add_argument('--index', type=str, default=INDEX_NAME,
                        help='OpenSearch index name')
    
    args = parser.parse_args()
    
    try:
        # Create OpenSearch client
        client = create_opensearch_client(args.host, args.port, args.user, args.password)
        
        # Check if OpenSearch is running
        if not client.ping():
            print("Could not connect to OpenSearch. Please check if it's running.")
            return
        
        # Create index
        create_index(client, args.index)
        
        # Generate and index documents
        bulk_index_documents(client, args.num_docs, args.index)
        
        print(f"Completed indexing {args.num_docs} documents to {args.index}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
