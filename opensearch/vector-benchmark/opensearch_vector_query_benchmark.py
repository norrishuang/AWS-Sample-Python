#!/usr/bin/env python3
"""
OpenSearch Vector Query Benchmark

This script performs concurrent vector search queries against OpenSearch
using process-based parallelism and collects performance metrics including QPS and latency percentiles.
It also supports evaluating recall by comparing OpenSearch results with exact nearest neighbors.
"""

import argparse
import datetime
import numpy as np
import time
import statistics
import multiprocessing
from collections import deque
from opensearchpy import OpenSearch, RequestsHttpConnection
import sys
import os
from sklearn.neighbors import NearestNeighbors

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
DEFAULT_RECALL_SAMPLE = 1000  # Number of documents to sample for recall evaluation

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

def warmup_knn_index(host, port, index_name, user, password, use_ssl=True, aws_auth=False, region=None):
    """
    Warm up the KNN index by calling the OpenSearch KNN warmup API.
    This ensures the index is loaded into memory before benchmarking.
    
    Args:
        host: OpenSearch host
        port: OpenSearch port
        index_name: Name of the index
        user: OpenSearch username
        password: OpenSearch password
        use_ssl: Whether to use HTTPS
        aws_auth: Whether to use AWS IAM authentication
        region: AWS region (required for AWS auth)
    """
    print(f"Warming up KNN index for {index_name}...")
    
    try:
        import requests
        from requests.auth import HTTPBasicAuth
        
        protocol = "https" if use_ssl else "http"
        url = f"{protocol}://{host}:{port}/_plugins/_knn/warmup/{index_name}?pretty"
        
        print(f"Calling KNN warmup API: {url}")
        
        if aws_auth:
            try:
                from requests_aws4auth import AWS4Auth
                import boto3
                
                # Get AWS credentials
                session = boto3.Session()
                credentials = session.get_credentials()
                auth = AWS4Auth(
                    credentials.access_key,
                    credentials.secret_key,
                    region,
                    'es',
                    session_token=credentials.token
                )
                
                response = requests.get(
                    url, 
                    auth=auth, 
                    timeout=60,
                    verify=False  # Skip SSL verification for development
                )
            except ImportError:
                print("Warning: AWS authentication requires 'requests_aws4auth' package.")
                print("Skipping KNN warmup.")
                return
        else:
            response = requests.get(
                url, 
                auth=HTTPBasicAuth(user, password), 
                timeout=60,
                verify=False  # Skip SSL verification for development
            )
        
        response.raise_for_status()  # Raise an error for HTTP issues
        print(f"KNN warmup completed successfully: {response.text}")
        
    except ImportError:
        print("Warning: 'requests' package not found. Skipping KNN warmup.")
        print("Install with: pip install requests")
    except Exception as e:
        print(f"Warning: KNN warmup failed: {e}")
        print("Continuing without warmup. This may affect initial query performance.")

def fetch_all_vectors(client, index_name, sample_size=None):
    """
    Fetch all document vectors from the index for recall evaluation.
    
    Args:
        client: OpenSearch client
        index_name: Name of the index
        sample_size: Number of documents to sample (None for all)
    
    Returns:
        Tuple of (document_ids, vectors)
    """
    print(f"Fetching vectors from index {index_name} for recall evaluation...")
    
    # First, get the total count of documents
    count_response = client.count(index=index_name)
    total_docs = count_response['count']
    
    if sample_size and sample_size < total_docs:
        print(f"Sampling {sample_size} documents out of {total_docs} total documents")
        size = sample_size
    else:
        print(f"Fetching all {total_docs} documents")
        size = total_docs
    
    # Fetch documents with their vectors
    query = {
        "size": size,
        "_source": ["content_vector"],
        "query": {
            "match_all": {}
        }
    }
    
    response = client.search(
        body=query,
        index=index_name
    )
    
    # Extract document IDs and vectors
    doc_ids = []
    vectors = []
    
    for hit in response['hits']['hits']:
        doc_ids.append(hit['_id'])
        vectors.append(hit['_source']['content_vector'])
    
    print(f"Successfully fetched {len(vectors)} document vectors")
    return np.array(doc_ids), np.array(vectors)

def build_exact_knn_index(vectors):
    """
    Build an exact KNN index using scikit-learn for ground truth.
    
    Args:
        vectors: Document vectors
    
    Returns:
        Fitted NearestNeighbors model
    """
    print("Building exact KNN index for ground truth...")
    # Use cosine similarity (inner product for normalized vectors)
    knn = NearestNeighbors(algorithm='brute', metric='cosine')
    knn.fit(vectors)
    return knn

def calculate_recall(ground_truth_ids, opensearch_ids):
    """
    Calculate recall@k by comparing OpenSearch results with ground truth.
    
    Args:
        ground_truth_ids: List of document IDs from exact KNN
        opensearch_ids: List of document IDs from OpenSearch
    
    Returns:
        Recall value (0.0 to 1.0)
    """
    # Convert to sets for intersection
    ground_truth_set = set(ground_truth_ids)
    opensearch_set = set(opensearch_ids)
    
    # Calculate recall
    if not ground_truth_set:
        return 0.0
    
    intersection = ground_truth_set.intersection(opensearch_set)
    recall = len(intersection) / len(ground_truth_set)
    
    return recall

def worker_process(args):
    """Worker function to execute queries until stop flag is set."""
    host, port, user, password, index_name, vector_dimension, k, worker_id, stop_flag, result_queue, evaluate_recall, all_doc_ids, all_vectors, exact_knn, random_queries = args
    
    # Create client for this process
    client = create_opensearch_client(host, port, user, password)
    
    query_id = 0
    while not stop_flag.value:
        query_id += 1
        
        # For recall evaluation, use a vector from the sample set as query
        # For performance-only testing, generate a random vector
        if evaluate_recall and all_vectors is not None and len(all_vectors) > 0 and not random_queries:
            # Select a random vector from the sample set
            random_idx = np.random.randint(0, len(all_vectors))
            vector = all_vectors[random_idx].tolist()
            query_doc_id = all_doc_ids[random_idx]  # Remember which document this vector belongs to
        else:
            # Generate random vector for performance testing
            vector = np.random.uniform(-1, 1, vector_dimension).tolist()
            query_doc_id = None
        
        # Prepare query
        query = {
            "size": k + 1,  # Add 1 to account for the query document itself when evaluating recall
            "query": {
                "knn": {
                    "content_vector": {
                        "vector": vector,
                        "k": k + 1
                    }
                }
            }
        }
        
        try:
            start_time = time.time()
            response = client.search(body=query, index=index_name)
            end_time = time.time()
            
            # Extract the took field (in milliseconds)
            took_ms = response.get('took', 0)
            
            # Calculate recall if enabled
            recall = None
            if evaluate_recall and exact_knn is not None:
                # Get OpenSearch result IDs
                opensearch_ids = [hit['_id'] for hit in response['hits']['hits']]
                
                if query_doc_id is not None:
                    # Remove the query document from results if it's from the sample set
                    opensearch_ids = [doc_id for doc_id in opensearch_ids if doc_id != query_doc_id]
                
                opensearch_ids = opensearch_ids[:k]  # Limit to k results
                
                if random_queries:
                    # For random queries, compare against exact KNN on the sample set
                    vector_np = np.array(vector).reshape(1, -1)
                    _, indices = exact_knn.kneighbors(vector_np, n_neighbors=k)
                    ground_truth_ids = all_doc_ids[indices[0]]
                else:
                    # For sample set queries, get ground truth by excluding the query document
                    vector_np = np.array(vector).reshape(1, -1)
                    _, indices = exact_knn.kneighbors(vector_np, n_neighbors=k+1)  # +1 to account for self
                    
                    # Filter out the query document from ground truth
                    ground_truth_indices = [idx for idx in indices[0] if all_doc_ids[idx] != query_doc_id][:k]
                    ground_truth_ids = all_doc_ids[ground_truth_indices]
                
                # Calculate recall
                recall = calculate_recall(ground_truth_ids, opensearch_ids)
            
            # Put result in queue
            result_queue.put((took_ms, recall))
                
        except Exception as e:
            print(f"Query {worker_id}-{query_id} error: {e}")

def run_benchmark(host, port, user, password, index_name, vector_dimension, k, concurrency, duration_seconds, evaluate_recall=False, recall_sample_size=None, random_queries=False, aws_auth=False, region=None):
    """Run the benchmark with specified parameters."""
    print(f"Starting benchmark with {concurrency} concurrent processes for {duration_seconds} seconds...")
    
    # Warm up the KNN index before benchmarking
    warmup_knn_index(host, port, index_name, user, password, USE_SSL, aws_auth, region)
    
    # Setup multiprocessing manager for shared state
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()
    stop_flag = manager.Value('b', False)
    
    # Initialize recall evaluation data if needed
    all_doc_ids = None
    all_vectors = None
    exact_knn = None
    
    if evaluate_recall:
        try:
            # Create client for fetching vectors
            client = create_opensearch_client(host, port, user, password)
            
            # Fetch document vectors
            all_doc_ids, all_vectors = fetch_all_vectors(client, index_name, recall_sample_size)
            
            if len(all_vectors) < k + 1:
                print(f"Warning: Sample size ({len(all_vectors)}) is too small for k={k}. Need at least k+1 documents.")
                print("Continuing without recall evaluation")
                evaluate_recall = False
            else:
                # Build exact KNN index
                exact_knn = build_exact_knn_index(all_vectors)
                
                print(f"Recall evaluation prepared with {len(all_doc_ids)} documents")
                if not random_queries:
                    print("Using vectors from the sample set as query vectors for accurate recall evaluation")
                else:
                    print("Using random vectors for queries (note: this may result in artificially low recall)")
        except Exception as e:
            print(f"Error preparing recall evaluation: {e}")
            print("Continuing without recall evaluation")
            evaluate_recall = False
    
    # Start worker processes
    processes = []
    start_time = time.time()
    
    for i in range(concurrency):
        args = (host, port, user, password, index_name, vector_dimension, k, i, stop_flag, result_queue, evaluate_recall, all_doc_ids, all_vectors, exact_knn, random_queries)
        p = multiprocessing.Process(target=worker_process, args=(args,))
        p.daemon = True  # Set as daemon so they will terminate when main process exits
        p.start()
        processes.append(p)
    
    # Monitor progress
    latencies = []
    recalls = []
    try:
        while time.time() - start_time < duration_seconds:
            # Collect results from queue
            while not result_queue.empty():
                result = result_queue.get_nowait()
                latency, recall = result
                latencies.append(latency)
                if recall is not None:
                    recalls.append(recall)
            
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
            
            # Calculate current recall if available
            avg_recall = sum(recalls) / len(recalls) if recalls else 0
            
            # Print progress update
            status_line = f"\rRunning: {elapsed:.1f}s | " \
                          f"Queries: {len(latencies)} | " \
                          f"QPS: {current_qps:.1f} | " \
                          f"P50: {p50:.1f}ms | " \
                          f"P99: {p99:.1f}ms"
            
            if evaluate_recall:
                status_line += f" | Avg Recall@{k}: {avg_recall:.4f}"
                
            sys.stdout.write(status_line)
            sys.stdout.flush()
            
            time.sleep(1)
    
    finally:
        # Signal workers to stop
        stop_flag.value = True
        
        # Wait a moment for processes to finish current queries
        time.sleep(2)
        
        # Collect any remaining results
        while not result_queue.empty():
            result = result_queue.get_nowait()
            latency, recall = result
            latencies.append(latency)
            if recall is not None:
                recalls.append(recall)
        
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
    
    # Add recall statistics if available
    if recalls:
        stats["recall"] = {
            "min": min(recalls),
            "max": max(recalls),
            "mean": statistics.mean(recalls),
            "p50": sorted(recalls)[len(recalls) // 2],
            "p90": sorted(recalls)[int(len(recalls) * 0.9)],
            "p95": sorted(recalls)[int(len(recalls) * 0.95)],
            "p99": sorted(recalls)[int(len(recalls) * 0.99)]
        }
    
    return stats

def main():
    """Main function to parse arguments and execute the benchmark."""
    parser = argparse.ArgumentParser(description='Benchmark OpenSearch vector search performance')
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
                        help=f'Number of nearest neighbors to retrieve (default: {DEFAULT_K})')
    parser.add_argument('--evaluate-recall', action='store_true',
                        help='Evaluate recall by comparing with exact nearest neighbors')
    parser.add_argument('--recall-sample', type=int, default=DEFAULT_RECALL_SAMPLE,
                        help=f'Number of documents to sample for recall evaluation (default: {DEFAULT_RECALL_SAMPLE})')
    parser.add_argument('--random-queries', action='store_true',
                        help='Use random vectors for queries instead of vectors from the sample set (not recommended for recall evaluation)')
    parser.add_argument('--skip-warmup', action='store_true',
                        help='Skip the KNN index warmup step before benchmarking')
    
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
        
        # Check if scikit-learn is installed when recall evaluation is requested
        if args.evaluate_recall:
            try:
                import sklearn
                print("Recall evaluation enabled. This will require additional memory to store document vectors.")
                
                if args.random_queries:
                    print("WARNING: Using random query vectors with recall evaluation will produce inaccurate results.")
                    print("The recall will be artificially low because the ground truth is based on the sample set.")
                    print("Consider removing the --random-queries flag for accurate recall evaluation.")
            except ImportError:
                print("Error: Recall evaluation requires scikit-learn package.")
                print("Please install it with: pip install scikit-learn")
                return
        
        # Warm up the KNN index if not skipped
        if not args.skip_warmup:
            warmup_knn_index(
                host=args.host,
                port=args.port,
                index_name=args.index,
                user=args.user,
                password=args.password,
                use_ssl=USE_SSL,
                aws_auth=args.aws_auth,
                region=args.region
            )
        
        # Run benchmark
        results = run_benchmark(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            index_name=args.index,
            vector_dimension=args.dimension,
            k=args.k,
            concurrency=args.concurrency,
            duration_seconds=args.duration,
            evaluate_recall=args.evaluate_recall,
            recall_sample_size=args.recall_sample,
            random_queries=args.random_queries,
            aws_auth=args.aws_auth,
            region=args.region
        )
        
        if results:
            # Print results
            print("\n\n" + "="*50)
            print("BENCHMARK RESULTS")
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
            
            # Print recall statistics if available
            if 'recall' in results:
                print("\nRecall@{} Statistics:".format(args.k))
                print(f"  Min: {results['recall']['min']:.4f}")
                print(f"  Mean: {results['recall']['mean']:.4f}")
                print(f"  P50: {results['recall']['p50']:.4f}")
                print(f"  P90: {results['recall']['p90']:.4f}")
                print(f"  P95: {results['recall']['p95']:.4f}")
                print(f"  P99: {results['recall']['p99']:.4f}")
                print(f"  Max: {results['recall']['max']:.4f}")
                
                if args.random_queries and args.evaluate_recall:
                    print("\nNOTE: Recall was evaluated using random query vectors against a sample set.")
                    print("This approach may not accurately represent true recall performance.")
                    print("For more accurate recall evaluation, run without the --random-queries flag.")
                else:
                    print("\nNOTE: Recall was evaluated using vectors from the sample set as queries,")
                    print("which provides a more accurate assessment of the ANN algorithm's performance.")
            
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
        
        if results:
            # Print results
            print("\n\n" + "="*50)
            print("BENCHMARK RESULTS")
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
            
            # Print recall statistics if available
            if 'recall' in results:
                print("\nRecall@{} Statistics:".format(args.k))
                print(f"  Min: {results['recall']['min']:.4f}")
                print(f"  Mean: {results['recall']['mean']:.4f}")
                print(f"  P50: {results['recall']['p50']:.4f}")
                print(f"  P90: {results['recall']['p90']:.4f}")
                print(f"  P95: {results['recall']['p95']:.4f}")
                print(f"  P99: {results['recall']['p99']:.4f}")
                print(f"  Max: {results['recall']['max']:.4f}")
            
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
