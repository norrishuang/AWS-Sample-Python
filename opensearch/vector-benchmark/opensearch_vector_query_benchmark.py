#!/usr/bin/env python3
"""
OpenSearch Vector Query Benchmark

This script performs concurrent vector search queries against OpenSearch
and collects performance metrics including QPS and latency percentiles.
"""

import argparse
import datetime
import json
import numpy as np
import random
import string
import time
import threading
import queue
import statistics
from collections import deque
from opensearchpy import OpenSearch, RequestsHttpConnection
import sys
import math
from concurrent.futures import ThreadPoolExecutor

# Default OpenSearch connection settings
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'admin'
USE_SSL = True  # Amazon OpenSearch Service requires HTTPS

# Default index settings
INDEX_NAME = 'vector_benchmark'
VECTOR_DIMENSION = 1536
DEFAULT_CONCURRENCY = 10
DEFAULT_DURATION = 60  # seconds
DEFAULT_K = 10  # Number of nearest neighbors to retrieve

class LatencyTracker:
    """Track and calculate latency statistics."""
    
    def __init__(self, window_size=1000):
        self.latencies = deque(maxlen=window_size)
        self.lock = threading.Lock()
    
    def add_latency(self, latency_ms):
        """Add a latency measurement."""
        with self.lock:
            self.latencies.append(latency_ms)
    
    def get_percentile(self, percentile):
        """Calculate the specified percentile of latencies."""
        with self.lock:
            if not self.latencies:
                return 0
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * percentile / 100)
            return sorted_latencies[idx]
    
    def get_stats(self):
        """Get all latency statistics."""
        with self.lock:
            if not self.latencies:
                return {
                    "count": 0,
                    "min": 0,
                    "max": 0,
                    "mean": 0,
                    "p50": 0,
                    "p90": 0,
                    "p95": 0,
                    "p99": 0
                }
            
            sorted_latencies = sorted(self.latencies)
            return {
                "count": len(sorted_latencies),
                "min": min(sorted_latencies),
                "max": max(sorted_latencies),
                "mean": statistics.mean(sorted_latencies),
                "p50": self.get_percentile(50),
                "p90": self.get_percentile(90),
                "p95": self.get_percentile(95),
                "p99": self.get_percentile(99)
            }

class QueryBenchmark:
    """Benchmark OpenSearch vector queries."""
    
    def __init__(self, client, index_name, vector_dimension, k=10):
        self.client = client
        self.index_name = index_name
        self.vector_dimension = vector_dimension
        self.k = k
        self.latency_tracker = LatencyTracker()
        self.query_count = 0
        self.query_count_lock = threading.Lock()
        self.running = False
        self.start_time = None
        self.end_time = None
    
    def generate_random_vector(self):
        """Generate a random vector with the specified dimension."""
        return np.random.uniform(-1, 1, self.vector_dimension).tolist()
    
    def perform_vector_search(self):
        """Perform a single vector search query."""
        print("Starting vector search query...")
        query_vector = self.generate_random_vector()
        print(f"Generated random vector with dimension {len(query_vector)}")
        
        query = {
            "size": self.k,
            "query": {
                "knn": {
                    "content_vector": {
                        "vector": query_vector,
                        "k": self.k
                    }
                }
            }
        }
        print(f"Query prepared: searching for {self.k} nearest neighbors")
        
        try:
            print(f"Sending search request to OpenSearch index '{self.index_name}'...")
            start_time = time.time()
            print(f"Request started at: {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
            response = self.client.search(
                body=query,
                index=self.index_name
            )
            end_time = time.time()
            print(f"Request completed at: {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
            print(f"Search completed in {(end_time - start_time) * 1000:.2f} ms")
            
            # Extract the took field (in milliseconds)
            took_ms = response.get('took', 0)
            print(f"OpenSearch reported query time: {took_ms} ms")
            print(f"Found {len(response['hits']['hits'])} results")
            
            # Update metrics
            self.latency_tracker.add_latency(took_ms)
            with self.query_count_lock:
                self.query_count += 1
                print(f"Total queries completed so far: {self.query_count}")
            
            return True
        except Exception as e:
            print(f"Search error: {e}")
            print(f"Error type: {type(e).__name__}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return False
    
    def worker(self):
        """Worker function for query threads."""
        print(f"Worker thread started at {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
        while self.running:
            print(f"Worker executing query at {datetime.datetime.now().strftime('%H:%M:%S.%f')}")
            result = self.perform_vector_search()
            print(f"Query completed with result: {result}")
            if not result:
                print("Query failed, sleeping for 1 second before retry...")
                time.sleep(1)
    
    def run_benchmark(self, concurrency, duration_seconds):
        """Run the benchmark with specified concurrency for a set duration."""
        self.running = True
        self.start_time = time.time()
        self.query_count = 0
        
        print(f"Starting benchmark with {concurrency} concurrent threads for {duration_seconds} seconds...")
        print(f"Vector dimension: {self.vector_dimension}, K: {self.k}")
        print(f"Benchmark start time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Create and start worker threads
        print(f"Creating {concurrency} worker threads...")
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            print("ThreadPoolExecutor created, submitting worker tasks...")
            futures = [executor.submit(self.worker) for _ in range(concurrency)]
            print(f"{len(futures)} worker tasks submitted")
            
            # Monitor and report progress
            try:
                elapsed = 0
                while elapsed < duration_seconds:
                    time.sleep(1)
                    elapsed = time.time() - self.start_time
                    current_qps = self.query_count / elapsed if elapsed > 0 else 0
                    stats = self.latency_tracker.get_stats()
                    
                    # Print progress update
                    sys.stdout.write(f"\rRunning: {elapsed:.1f}s | "
                                    f"Queries: {self.query_count} | "
                                    f"QPS: {current_qps:.1f} | "
                                    f"P50: {stats['p50']:.1f}ms | "
                                    f"P99: {stats['p99']:.1f}ms")
                    sys.stdout.flush()
                    print("")  # Add a newline for better debug output visibility
            
            finally:
                # Stop the benchmark
                print("\nBenchmark duration completed, stopping workers...")
                self.running = False
                self.end_time = time.time()
                print(f"Benchmark end time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Calculate final results
        actual_duration = self.end_time - self.start_time
        qps = self.query_count / actual_duration if actual_duration > 0 else 0
        print(f"Benchmark completed. Total queries: {self.query_count}, Duration: {actual_duration:.2f}s, QPS: {qps:.2f}")
        
        return {
            "queries": self.query_count,
            "duration_seconds": actual_duration,
            "qps": qps,
            "latency": self.latency_tracker.get_stats()
        }

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
                        help=f'Number of concurrent query threads (default: {DEFAULT_CONCURRENCY})')
    parser.add_argument('--duration', type=int, default=DEFAULT_DURATION,
                        help=f'Benchmark duration in seconds (default: {DEFAULT_DURATION})')
    parser.add_argument('--k', type=int, default=DEFAULT_K,
                        help=f'Number of nearest neighbors to retrieve (default: {DEFAULT_K})')
    
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
            print(f"Creating OpenSearch client with basic auth (user: {args.user})...")
            client = create_opensearch_client(args.host, args.port, args.user, args.password)
        
        # Check if OpenSearch is running
        try:
            print("Testing connection to OpenSearch...")
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
        print(f"Checking if index '{args.index}' exists...")
        if not client.indices.exists(index=args.index):
            print(f"Error: Index '{args.index}' does not exist.")
            print("Please create the index first using opensearch_vector_benchmark.py")
            return
        else:
            print(f"Index '{args.index}' exists.")
            
            # Check index stats
            try:
                print("Retrieving index statistics...")
                index_stats = client.indices.stats(index=args.index)
                doc_count = index_stats['indices'][args.index]['total']['docs']['count']
                print(f"Index '{args.index}' contains {doc_count} documents")
                if doc_count == 0:
                    print("Warning: Index exists but contains no documents!")
            except Exception as e:
                print(f"Error checking index stats: {e}")
        
        # Test a simple query
        try:
            print("Testing a simple search query...")
            test_query = {"query": {"match_all": {}}, "size": 1}
            test_result = client.search(body=test_query, index=args.index)
            print(f"Simple query successful, found {test_result['hits']['total']['value']} documents")
            if test_result['hits']['total']['value'] > 0:
                print("Sample document fields:", list(test_result['hits']['hits'][0]['_source'].keys()))
                if 'content_vector' in test_result['hits']['hits'][0]['_source']:
                    vector_dim = len(test_result['hits']['hits'][0]['_source']['content_vector'])
                    print(f"Vector dimension in index: {vector_dim}")
                    if vector_dim != args.dimension:
                        print(f"Warning: Specified dimension ({args.dimension}) doesn't match index dimension ({vector_dim})")
                        print(f"Updating dimension to {vector_dim}")
                        args.dimension = vector_dim
        except Exception as e:
            print(f"Error with simple test query: {e}")
            return
        
        # Create and run benchmark
        print("Creating benchmark object...")
        benchmark = QueryBenchmark(client, args.index, args.dimension, args.k)
        print("Starting benchmark run...")
        results = benchmark.run_benchmark(args.concurrency, args.duration)
        
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
    main()
