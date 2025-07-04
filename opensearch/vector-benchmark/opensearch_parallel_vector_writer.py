#!/usr/bin/env python3
"""
OpenSearch Parallel Vector Data Writer

This script generates random vector data with timestamps and writes it to OpenSearch
using parallel processing for improved performance. Each document includes a write_timestamp
field that records when the data was written.

Required dependencies:
pip install opensearch-py numpy faker scikit-learn

For AWS IAM authentication:
pip install requests_aws4auth boto3
"""

import argparse
import datetime
import json
import random
import string
import time
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys
import os

# Check for required dependencies
try:
    import numpy as np
    from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
    from faker import Faker
except ImportError as e:
    print(f"Error: Missing required dependency - {e}")
    print("Please install required packages:")
    print("pip install opensearch-py numpy faker")
    print("For AWS IAM authentication:")
    print("pip install requests_aws4auth boto3")
    sys.exit(1)

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
VECTOR_DIMENSION = 1536  # Default vector dimension
DEFAULT_BATCH_SIZE = 100
DEFAULT_WORKERS = 4

def create_opensearch_client(host, port, username, password, aws_auth=False, region='us-east-1'):
    """Create and return an OpenSearch client."""
    if aws_auth:
        try:
            import boto3
            from requests_aws4auth import AWS4Auth
            
            credentials = boto3.Session().get_credentials()
            awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
            
            client = OpenSearch(
                hosts=[{'host': host, 'port': int(port)}],
                http_auth=awsauth,
                use_ssl=USE_SSL,
                verify_certs=True,
                connection_class=RequestsHttpConnection,
                timeout=60
            )
        except ImportError:
            print("Error: boto3 and requests_aws4auth are required for AWS authentication.")
            print("Install them with: pip install boto3 requests_aws4auth")
            sys.exit(1)
    else:
        client = OpenSearch(
            hosts=[{'host': host, 'port': int(port)}],
            http_auth=(username, password),
            use_ssl=USE_SSL,
            verify_certs=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
            timeout=60
        )
    return client

def create_index(client, index_name, vector_dimension=VECTOR_DIMENSION, num_shards=12, num_replicas=1):
    """Create an OpenSearch index with vector field configuration."""
    # Check if index exists
    if client.indices.exists(index=index_name):
        print(f"Index {index_name} already exists. Skipping index creation.")
        return {"acknowledged": True, "index": index_name, "status": "exists"}
    
    # If index doesn't exist, create it
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100,
                "number_of_shards": num_shards,
                "number_of_replicas": num_replicas
            }
        },
        "mappings": {
            "properties": {
                "content": {"type": "text"},
                "platform": {"type": "keyword"},
                "date": {"type": "date"},
                "tag": {"type": "keyword"},
                "write_timestamp": {"type": "date"},  # New field for write timestamp
                "content_vector": {
                    "type": "knn_vector",
                    "dimension": vector_dimension,
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
                },
                "content_sparse_vector": {
                    "type": "rank_features"
                }
            }
        }
    }
    
    # Create the index
    response = client.indices.create(index=index_name, body=index_body)
    print(f"Index created: {response}")
    return response

def generate_sparse_vector(min_terms=20, max_terms=40):
    """Generate a sparse vector with random terms and weights."""
    # Generate a vocabulary of random terms
    vocab_size = 1000
    vocab = [f"term_{i}" for i in range(vocab_size)]
    
    # Randomly select terms
    num_terms = random.randint(min_terms, max_terms)
    selected_terms = random.sample(vocab, num_terms)
    
    # Generate sparse vector
    sparse_vector = {}
    for term in selected_terms:
        sparse_vector[term] = random.uniform(0.1, 1.0)
    
    return sparse_vector

def generate_document(doc_id, vector_dimension, min_sparse_terms, max_sparse_terms):
    """Generate a single document with vector data and timestamp."""
    platforms = ["web", "mobile", "desktop", "api", "iot", "cloud"]
    tags = ["news", "tech", "business", "sports", "entertainment", "science", "health", "politics"]
    
    # Generate current timestamp for write_timestamp
    write_timestamp = datetime.datetime.utcnow().isoformat() + "Z"
    
    document = {
        "_id": doc_id,
        "_index": INDEX_NAME,
        "_source": {
            "content": fake.text(max_nb_chars=200),
            "platform": random.choice(platforms),
            "date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
            "tag": random.sample(tags, random.randint(1, 3)),
            "write_timestamp": write_timestamp,  # Add write timestamp
            "content_vector": np.random.rand(vector_dimension).astype(np.float32).tolist(),
            "content_sparse_vector": generate_sparse_vector(min_sparse_terms, max_sparse_terms)
        }
    }
    
    return document

def write_batch_worker(args):
    """Worker function to write a batch of documents."""
    (worker_id, start_id, batch_size, host, port, user, password, aws_auth, region, 
     index_name, vector_dimension, min_sparse_terms, max_sparse_terms) = args
    
    try:
        # Create OpenSearch client for this worker
        client = create_opensearch_client(host, port, user, password, aws_auth, region)
        
        # Generate documents for this batch
        documents = []
        for i in range(batch_size):
            doc_id = start_id + i
            doc = generate_document(doc_id, vector_dimension, min_sparse_terms, max_sparse_terms)
            documents.append(doc)
        
        # Bulk index the documents
        start_time = time.time()
        success_count, failed_items = helpers.bulk(
            client,
            documents,
            index=index_name,
            chunk_size=batch_size,
            request_timeout=60
        )
        end_time = time.time()
        
        duration = end_time - start_time
        docs_per_second = batch_size / duration if duration > 0 else 0
        
        return {
            'worker_id': worker_id,
            'success_count': success_count,
            'failed_count': len(failed_items) if failed_items else 0,
            'duration': duration,
            'docs_per_second': docs_per_second,
            'batch_size': batch_size
        }
        
    except Exception as e:
        return {
            'worker_id': worker_id,
            'error': str(e),
            'success_count': 0,
            'failed_count': batch_size,
            'duration': 0,
            'docs_per_second': 0,
            'batch_size': batch_size
        }

def main():
    parser = argparse.ArgumentParser(description='OpenSearch Parallel Vector Data Writer')
    parser.add_argument('--num-docs', type=int, default=1000, help='Number of documents to generate and index')
    parser.add_argument('--host', default=OPENSEARCH_HOST, help='OpenSearch host')
    parser.add_argument('--port', type=int, default=OPENSEARCH_PORT, help='OpenSearch port')
    parser.add_argument('--user', default=OPENSEARCH_USER, help='OpenSearch username')
    parser.add_argument('--password', default=OPENSEARCH_PASSWORD, help='OpenSearch password')
    parser.add_argument('--index', default=INDEX_NAME, help='OpenSearch index name')
    parser.add_argument('--aws-auth', action='store_true', help='Use AWS IAM authentication')
    parser.add_argument('--region', default='us-east-1', help='AWS region (required for AWS auth)')
    parser.add_argument('--dimension', type=int, default=VECTOR_DIMENSION, help='Vector dimension')
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE, help='Batch size for bulk indexing')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS, help='Number of parallel workers')
    parser.add_argument('--min-sparse-terms', type=int, default=20, help='Minimum number of terms in sparse vector')
    parser.add_argument('--max-sparse-terms', type=int, default=40, help='Maximum number of terms in sparse vector')
    parser.add_argument('--shards', type=int, default=12, help='Number of primary shards')
    parser.add_argument('--replicas', type=int, default=1, help='Number of replica shards')
    
    args = parser.parse_args()
    
    print(f"Starting parallel vector data writing with {args.workers} workers...")
    print(f"Target documents: {args.num_docs}")
    print(f"Batch size: {args.batch_size}")
    print(f"Vector dimension: {args.dimension}")
    print(f"OpenSearch: {args.host}:{args.port}")
    print(f"Index: {args.index}")
    print(f"Authentication: {'AWS IAM' if args.aws_auth else 'Basic'}")
    
    # Create OpenSearch client for index management
    client = create_opensearch_client(args.host, args.port, args.user, args.password, args.aws_auth, args.region)
    
    # Test connection
    try:
        info = client.info()
        print(f"Connected to OpenSearch: {info['version']['number']}")
    except Exception as e:
        print(f"Failed to connect to OpenSearch: {e}")
        sys.exit(1)
    
    # Create index
    try:
        create_index(client, args.index, args.dimension, args.shards, args.replicas)
    except Exception as e:
        print(f"Failed to create index: {e}")
        sys.exit(1)
    
    # Calculate batches
    total_batches = (args.num_docs + args.batch_size - 1) // args.batch_size
    print(f"Total batches: {total_batches}")
    
    # Prepare worker arguments
    worker_args = []
    doc_id_counter = 0
    
    for batch_idx in range(total_batches):
        # Calculate batch size for this batch (handle remainder)
        remaining_docs = args.num_docs - doc_id_counter
        current_batch_size = min(args.batch_size, remaining_docs)
        
        if current_batch_size <= 0:
            break
            
        worker_arg = (
            batch_idx,  # worker_id
            doc_id_counter,  # start_id
            current_batch_size,  # batch_size
            args.host,
            args.port,
            args.user,
            args.password,
            args.aws_auth,
            args.region,
            args.index,
            args.dimension,
            args.min_sparse_terms,
            args.max_sparse_terms
        )
        worker_args.append(worker_arg)
        doc_id_counter += current_batch_size
    
    # Execute parallel writing
    start_time = time.time()
    total_success = 0
    total_failed = 0
    total_duration = 0
    
    print(f"\nStarting parallel execution with {args.workers} workers...")
    
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        # Submit all tasks
        future_to_batch = {executor.submit(write_batch_worker, arg): arg[0] for arg in worker_args}
        
        # Process completed tasks
        completed = 0
        for future in as_completed(future_to_batch):
            batch_id = future_to_batch[future]
            try:
                result = future.result()
                total_success += result['success_count']
                total_failed += result['failed_count']
                total_duration += result['duration']
                
                completed += 1
                progress = (completed / len(worker_args)) * 100
                
                if 'error' in result:
                    print(f"Batch {batch_id}: ERROR - {result['error']}")
                else:
                    print(f"Batch {batch_id}: {result['success_count']} docs, "
                          f"{result['docs_per_second']:.1f} docs/sec, "
                          f"Progress: {progress:.1f}%")
                    
            except Exception as e:
                print(f"Batch {batch_id}: Exception - {e}")
                total_failed += args.batch_size
                completed += 1
    
    end_time = time.time()
    total_elapsed = end_time - start_time
    
    # Print final statistics
    print(f"\n{'='*60}")
    print("PARALLEL WRITING COMPLETED")
    print(f"{'='*60}")
    print(f"Total documents processed: {total_success + total_failed}")
    print(f"Successfully indexed: {total_success}")
    print(f"Failed: {total_failed}")
    print(f"Total elapsed time: {total_elapsed:.2f} seconds")
    print(f"Overall throughput: {total_success / total_elapsed:.1f} docs/sec")
    print(f"Workers used: {args.workers}")
    print(f"Average worker time: {total_duration / len(worker_args):.2f} seconds")
    
    if total_failed > 0:
        print(f"\nWarning: {total_failed} documents failed to index")
        sys.exit(1)
    else:
        print(f"\nAll documents successfully indexed to {args.index}")

if __name__ == "__main__":
    main()
