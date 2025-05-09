#!/usr/bin/env python3
"""
OpenSearch Vector Query Benchmark

This script performs concurrent vector search queries against OpenSearch
using process-based parallelism and collects performance metrics including QPS and latency percentiles.
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

def worker_process(args):
    """Worker function to execute queries until stop flag is set."""
    host, port, user, password, index_name, vector_dimension, k, worker_id, stop_flag, result_queue = args
    
    # Create client for this process
    client = create_opensearch_client(host, port, user, password)
    
    query_id = 0
    while not stop_flag.value:
        query_id += 1
        
        # Generate random vector
        vector = np.random.uniform(-1, 1, vector_dimension).tolist()
        
        # Prepare query
        query = {
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
        
        try:
            start_time = time.time()
            response = client.search(body=query, index=index_name)
            end_time = time.time()
            
            # Extract the took field (in milliseconds)
            took_ms = response.get('took', 0)
            
            # Put result in queue
            result_queue.put(took_ms)
                
        except Exception as e:
            print(f"Query {worker_id}-{query_id} error: {e}")

def run_benchmark(host, port, user, password, index_name, vector_dimension, k, concurrency, duration_seconds):
    """Run the benchmark with specified parameters."""
    print(f"Starting benchmark with {concurrency} concurrent processes for {duration_seconds} seconds...")
    
    # Setup multiprocessing manager for shared state
    manager = multiprocessing.Manager()
    result_queue = manager.Queue()
    stop_flag = manager.Value('b', False)
    
    # Start worker processes
    processes = []
    start_time = time.time()
    
    for i in range(concurrency):
        args = (host, port, user, password, index_name, vector_dimension, k, i, stop_flag, result_queue)
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
            duration_seconds=args.duration
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
