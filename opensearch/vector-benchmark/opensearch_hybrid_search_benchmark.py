#!/usr/bin/env python3
"""
OpenSearch Hybrid Search Benchmark

This script performs concurrent hybrid search queries (combining neural_sparse and knn) against OpenSearch
using process-based parallelism and collects performance metrics including QPS and latency percentiles.
"""

import argparse
import datetime
import numpy as np
import time
import statistics
import multiprocessing
import random
import string
from collections import deque
from opensearchpy import OpenSearch, RequestsHttpConnection
import sys
import os

# Default OpenSearch connection settings
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'admin'
USE_SSL = True  # Amazon OpenSearch Service requires HTTPS

# Default index settings
INDEX_NAME = 'vector_benchmark'
VECTOR_DIMENSION = 1536
DEFAULT_CONCURRENCY = 4  # Default to 4 processes
DEFAULT_DURATION = 60  # seconds
DEFAULT_K = 10  # Number of nearest neighbors to retrieve
DEFAULT_MIN_TERMS = 3  # Minimum number of terms to include in each query
DEFAULT_MAX_TERMS = 8  # Maximum number of terms to include in each query
DEFAULT_PIPELINE = "nlp-search-pipeline"  # Default search pipeline name

def create_opensearch_client(host, port, username, password):
    """Create and return an OpenSearch client."""
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

def get_sparse_vector_terms(client, index_name, sample_size=100):
    """
    Get a sample of sparse vector terms from the index to use in queries.
    
    Args:
        client: OpenSearch client
        index_name: Name of the index
        sample_size: Number of documents to sample
        
    Returns:
        list: A list of unique terms found in content_sparse_vector fields
    """
    try:
        # First, check the mapping to understand the structure
        mapping = client.indices.get_mapping(index=index_name)
        index_mappings = list(mapping.values())[0]
        
        # Check if content_sparse_vector exists
        properties = index_mappings.get('mappings', {}).get('properties', {})
        if 'content_sparse_vector' not in properties:
            print("Warning: content_sparse_vector field not found in index mapping")
            # Generate fallback terms
            return ['term' + str(i) for i in range(1, 101)]
        
        # Query to get a sample of documents
        query = {
            "size": sample_size,
            "query": {
                "match_all": {}
            }
        }
        
        response = client.search(body=query, index=index_name)
        
        # Extract all terms from content_sparse_vector fields
        all_terms = set()
        for hit in response.get('hits', {}).get('hits', []):
            source = hit.get('_source', {})
            sparse_vector = source.get('content_sparse_vector', {})
            if sparse_vector:
                all_terms.update(sparse_vector.keys())
        
        if not all_terms:
            print("Warning: No sparse vector terms found in sampled documents")
            # Generate fallback terms
            return ['term' + str(i) for i in range(1, 101)]
            
        print(f"Found {len(all_terms)} unique terms in content_sparse_vector fields")
        return list(all_terms)
    
    except Exception as e:
        print(f"Error getting sparse vector terms: {e}")
        # If we can't get real terms, generate some random ones
        return ['term' + str(i) for i in range(1, 101)]

def generate_random_hybrid_query(terms, vector_dimension, k, min_terms=DEFAULT_MIN_TERMS, max_terms=DEFAULT_MAX_TERMS):
    """
    Generate a random hybrid query combining neural_sparse and knn.
    
    Args:
        terms: List of available terms to choose from for neural_sparse
        vector_dimension: Dimension of the vector for knn
        k: Number of nearest neighbors to retrieve
        min_terms: Minimum number of terms to include in the query
        max_terms: Maximum number of terms to include in the query
        
    Returns:
        dict: A query body for OpenSearch using hybrid search
    """
    # Generate random vector for knn part
    vector = np.random.uniform(-1, 1, vector_dimension).tolist()
    
    # Generate neural_sparse part
    if not terms:
        # If no terms are available, use a simple knn query
        return {
            "size": k,
            "query": {
                "knn": {
                    "content_vector": {
                        "vector": vector,
                        "k": k
                    }
                }
            }
        }
    
    # Make sure we don't try to sample more terms than are available
    actual_max_terms = min(max_terms, len(terms))
    actual_min_terms = min(min_terms, actual_max_terms)
    
    # Select random number of terms between min_terms and max_terms
    num_terms = random.randint(actual_min_terms, actual_max_terms)
    
    # Select random terms
    selected_terms = random.sample(terms, num_terms)
    
    # Create query_tokens dictionary with random weights
    query_tokens = {}
    for term in selected_terms:
        # Generate a random weight between 1.0 and 5.0
        weight = round(random.uniform(1.0, 5.0), 7)
        query_tokens[term] = weight
    
    # Create the hybrid query
    query = {
        "size": k,
        "query": {
            "hybrid": {
                "queries": [
                    {
                        "neural_sparse": {
                            "content_sparse_vector": {
                                "query_tokens": query_tokens
                            }
                        }
                    },
                    {
                        "knn": {
                            "content_vector": {
                                "vector": vector,
                                "k": k
                            }
                        }
                    }
                ]
            }
        }
    }
    
    return query

def worker_process(args):
    """Worker function to execute hybrid search queries until stop flag is set."""
    host, port, user, password, index_name, terms, vector_dimension, min_terms, max_terms, k, pipeline_name, worker_id, stop_flag, result_queue = args
    
    # Create client for this process
    client = create_opensearch_client(host, port, user, password)
    
    query_id = 0
    while not stop_flag.value:
        query_id += 1
        
        try:
            # Generate random hybrid query
            query = generate_random_hybrid_query(terms, vector_dimension, k, min_terms, max_terms)
            
            # Set search parameters including pipeline
            search_params = {
                "body": query,
                "index": index_name
            }
            
            # Add pipeline if specified
            if pipeline_name:
                search_params["pipeline"] = pipeline_name
            
            start_time = time.time()
            response = client.search(**search_params)
            end_time = time.time()
            
            # Extract the took field (in milliseconds)
            took_ms = response.get('took', 0)
            
            # Put result in queue
            result_queue.put(took_ms)
                
        except Exception as e:
            print(f"Query {worker_id}-{query_id} error: {e}")
            # Sleep briefly to avoid flooding with errors
            time.sleep(0.1)

def run_benchmark(host, port, user, password, index_name, terms, vector_dimension, min_terms, max_terms, k, pipeline_name, concurrency, duration_seconds):
    """Run the benchmark with specified parameters."""
    print(f"Starting hybrid search benchmark with {concurrency} concurrent processes for {duration_seconds} seconds...")
    
    if pipeline_name:
        print(f"Using search pipeline: {pipeline_name}")
    
    if not terms:
        print("Warning: No valid terms found. Using fallback query strategy.")
    else:
        print(f"Using {min_terms}-{max_terms} terms per query from a vocabulary of {len(terms)} terms")
    
    # Setup multiprocessing manager for shared state
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()
    stop_flag = manager.Value('b', False)
    
    # Start worker processes
    processes = []
    start_time = time.time()
    
    for i in range(concurrency):
        args = (host, port, user, password, index_name, terms, vector_dimension, min_terms, max_terms, k, pipeline_name, i, stop_flag, result_queue)
        p = multiprocessing.Process(target=worker_process, args=(args,))
        p.daemon = True  # Set as daemon so they will terminate when main process exits
        p.start()
        processes.append(p)
    
    # Monitor progress
    latencies = []
    try:
        while time.time() - start_time < duration_seconds:
            # Collect results from queue
            while not result_queue.empty():
                latency = result_queue.get_nowait()
                latencies.append(latency)
            
            elapsed = time.time() - start_time
            current_qps = len(latencies) / elapsed if elapsed > 0 else 0
            
            # Calculate current statistics
            if latencies:
                sorted_latencies = sorted(latencies)
                p50 = sorted_latencies[len(sorted_latencies) // 2] if sorted_latencies else 0
                p99_idx = int(len(sorted_latencies) * 0.99) if sorted_latencies else 0
                p99 = sorted_latencies[p99_idx] if p99_idx < len(sorted_latencies) else 0
            else:
                p50 = 0
                p99 = 0
            
            # Print progress update
            sys.stdout.write(f"\rRunning: {elapsed:.1f}s | "
                            f"Queries: {len(latencies)} | "
                            f"QPS: {current_qps:.1f} | "
                            f"P50: {p50:.1f}ms | "
                            f"P99: {p99:.1f}ms")
            sys.stdout.flush()
            
            time.sleep(1)
    
    finally:
        # Signal workers to stop
        stop_flag.value = True
        
        # Wait a moment for processes to finish current queries
        time.sleep(2)
        
        # Collect any remaining results
        while not result_queue.empty():
            latency = result_queue.get_nowait()
            latencies.append(latency)
        
        # Terminate any remaining processes
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join(1)
    
    end_time = time.time()
    actual_duration = end_time - start_time
    
    # Calculate final results
    if not latencies:
        print("\nNo successful queries were executed.")
        return None
    
    sorted_latencies = sorted(latencies)
    qps = len(latencies) / actual_duration
    
    stats = {
        "queries": len(latencies),
        "duration_seconds": actual_duration,
        "qps": qps,
        "latency": {
            "min": min(sorted_latencies),
            "max": max(sorted_latencies),
            "mean": statistics.mean(sorted_latencies),
            "p50": sorted_latencies[len(sorted_latencies) // 2],
            "p90": sorted_latencies[int(len(sorted_latencies) * 0.9)],
            "p95": sorted_latencies[int(len(sorted_latencies) * 0.95)],
            "p99": sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }
    }
    
    return stats

def main():
    """Main function to parse arguments and execute the benchmark."""
    parser = argparse.ArgumentParser(description='Benchmark OpenSearch hybrid search performance')
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
    parser.add_argument('--aws-auth', action='store_true',
                        help='Use AWS IAM authentication instead of basic auth')
    parser.add_argument('--region', type=str, default='us-east-1',
                        help='AWS region for OpenSearch service (required for AWS auth)')
    parser.add_argument('--dimension', type=int, default=VECTOR_DIMENSION,
                        help=f'Vector dimension (default: {VECTOR_DIMENSION})')
    parser.add_argument('--concurrency', type=int, default=DEFAULT_CONCURRENCY,
                        help=f'Number of concurrent processes (default: {DEFAULT_CONCURRENCY})')
    parser.add_argument('--duration', type=int, default=DEFAULT_DURATION,
                        help=f'Benchmark duration in seconds (default: {DEFAULT_DURATION})')
    parser.add_argument('--k', type=int, default=DEFAULT_K,
                        help=f'Number of results to retrieve (default: {DEFAULT_K})')
    parser.add_argument('--min-terms', type=int, default=DEFAULT_MIN_TERMS,
                        help=f'Minimum number of terms in each query (default: {DEFAULT_MIN_TERMS})')
    parser.add_argument('--max-terms', type=int, default=DEFAULT_MAX_TERMS,
                        help=f'Maximum number of terms in each query (default: {DEFAULT_MAX_TERMS})')
    parser.add_argument('--sample-size', type=int, default=100,
                        help='Number of documents to sample for term extraction (default: 100)')
    parser.add_argument('--pipeline', type=str, default=DEFAULT_PIPELINE,
                        help=f'Search pipeline name (default: {DEFAULT_PIPELINE}). Use empty string to disable.')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode with more verbose output')
    
    args = parser.parse_args()
    
    try:
        # Create OpenSearch client for initial checks
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
        
        # Check if index exists
        if not client.indices.exists(index=args.index):
            print(f"Error: Index '{args.index}' does not exist.")
            print("Please create the index first using opensearch_vector_benchmark.py")
            return
        
        # Check if pipeline exists if specified
        pipeline_name = args.pipeline
        if pipeline_name:
            try:
                # Try to get the pipeline to verify it exists
                client.ingest.get_pipeline(id=pipeline_name)
                print(f"Search pipeline '{pipeline_name}' found.")
            except Exception as e:
                print(f"Warning: Search pipeline '{pipeline_name}' not found or not accessible: {e}")
                print("Will proceed without using a pipeline.")
                pipeline_name = None
        
        # Get sparse vector terms from the index
        print(f"Sampling documents to extract sparse vector terms...")
        terms = get_sparse_vector_terms(client, args.index, args.sample_size)
        
        # Debug: Show a sample query if debug mode is enabled
        if args.debug:
            sample_query = generate_random_hybrid_query(
                terms, 
                args.dimension,
                args.k,
                min(args.min_terms, len(terms) if terms else 0), 
                min(args.max_terms, len(terms) if terms else 0)
            )
            print("\nSample query that will be used:")
            import json
            print(json.dumps(sample_query, indent=2))
            print()
        
        # Run benchmark
        results = run_benchmark(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            index_name=args.index,
            terms=terms,
            vector_dimension=args.dimension,
            min_terms=args.min_terms,
            max_terms=args.max_terms,
            k=args.k,
            pipeline_name=pipeline_name,
            concurrency=args.concurrency,
            duration_seconds=args.duration
        )
        
        if results:
            # Print results
            print("\n\n" + "="*50)
            print("HYBRID SEARCH BENCHMARK RESULTS")
            print("="*50)
            print(f"Total queries: {results['queries']}")
            print(f"Duration: {results['duration_seconds']:.2f} seconds")
            print(f"QPS (queries per second): {results['qps']:.2f}")
            print("\nLatency Statistics (milliseconds):")
            print(f"  Min: {results['latency']['min']:.2f} ms")
            print(f"  Mean: {results['latency']['mean']:.2f} ms")
            print(f"  P50: {results['latency']['p50']:.2f} ms")
            print(f"  P90: {results['latency']['p90']:.2f} ms")
            print(f"  P95: {results['latency']['p95']:.2f} ms")
            print(f"  P99: {results['latency']['p99']:.2f} ms")
            print(f"  Max: {results['latency']['max']:.2f} ms")
            print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    # Set start method for multiprocessing
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        # Method already set
        pass
    main()
