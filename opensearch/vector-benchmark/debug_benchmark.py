#!/usr/bin/env python3
"""
Debug version of OpenSearch Vector Query Benchmark
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

def run_benchmark(client, index_name, vector_dimension, k, concurrency, duration_seconds):
    """Run the benchmark with specified parameters."""
    # Setup
    latency_tracker = LatencyTracker()
    query_count = 0
    query_count_lock = threading.Lock()
    running = True
    
    def generate_random_vector():
        """Generate a random vector with the specified dimension."""
        return np.random.uniform(-1, 1, vector_dimension).tolist()
    
    def perform_vector_search():
        """Perform a single vector search query."""
        nonlocal query_count
        query_vector = generate_random_vector()
        
        query = {
            "size": k,
            "query": {
                "knn": {
                    "content_vector": {
                        "vector": query_vector,
                        "k": k
                    }
                }
            }
        }
        
        try:
            start_time = time.time()
            response = client.search(
                body=query,
                index=index_name
            )
            end_time = time.time()
            
            # Extract the took field (in milliseconds)
            took_ms = response.get('took', 0)
            
            # Update metrics
            latency_tracker.add_latency(took_ms)
            with query_count_lock:
                query_count += 1
            
            return True
        except Exception as e:
            print(f"Search error: {e}")
            return False
    
    def worker():
        """Worker function for query threads."""
        while running:
            perform_vector_search()
    
    # Start benchmark
    print(f"Starting benchmark with {concurrency} concurrent threads for {duration_seconds} seconds...")
    start_time = time.time()
    
    # Create and start worker threads
    print("Creating thread pool...")
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        print(f"Submitting {concurrency} worker tasks...")
        futures = [executor.submit(worker) for _ in range(concurrency)]
        print(f"All {len(futures)} tasks submitted")
        
        # Monitor and report progress
        try:
            print("Starting monitoring loop...")
            elapsed = 0
            while elapsed < duration_seconds:
                time.sleep(1)
                elapsed = time.time() - start_time
                current_qps = query_count / elapsed if elapsed > 0 else 0
                stats = latency_tracker.get_stats()
                
                # Print progress update
                print(f"Progress: {elapsed:.1f}s | "
                      f"Queries: {query_count} | "
                      f"QPS: {current_qps:.1f} | "
                      f"P50: {stats['p50']:.1f}ms | "
                      f"P99: {stats['p99']:.1f}ms")
        
        finally:
            # Stop the benchmark
            print("Benchmark duration completed, stopping workers...")
            running = False
            end_time = time.time()
            print("Waiting for worker threads to complete...")
            
            # Wait for all futures to complete with a timeout
            for i, future in enumerate(futures):
                try:
                    print(f"Waiting for worker {i+1}/{len(futures)}...")
                    future.result(timeout=2)  # Wait up to 2 seconds for each future
                    print(f"Worker {i+1} completed")
                except Exception as e:
                    print(f"Worker {i+1} error or timeout: {e}")
    
    # Calculate final results
    print("Calculating final results...")
    actual_duration = end_time - start_time
    qps = query_count / actual_duration if actual_duration > 0 else 0
    stats = latency_tracker.get_stats()
    
    # Print results
    print("\n\n" + "="*50)
    print("BENCHMARK RESULTS")
    print("="*50)
    print(f"Total queries: {query_count}")
    print(f"Duration: {actual_duration:.2f} seconds")
    print(f"QPS (queries per second): {qps:.2f}")
    print("\nLatency Statistics (milliseconds):")
    print(f"  Min: {stats['min']:.2f} ms")
    print(f"  Mean: {stats['mean']:.2f} ms")
    print(f"  P50: {stats['p50']:.2f} ms")
    print(f"  P90: {stats['p90']:.2f} ms")
    print(f"  P95: {stats['p95']:.2f} ms")
    print(f"  P99: {stats['p99']:.2f} ms")
    print(f"  Max: {stats['max']:.2f} ms")
    print("="*50)
    
    return {
        "queries": query_count,
        "duration_seconds": actual_duration,
        "qps": qps,
        "latency": stats
    }

def main():
    """Main function to parse arguments and execute the benchmark."""
    parser = argparse.ArgumentParser(description='Debug OpenSearch vector search performance')
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
        print("Starting benchmark script...")
        
        # Create OpenSearch client
        print(f"Connecting to OpenSearch at {args.host}:{args.port}...")
        client = create_opensearch_client(args.host, args.port, args.user, args.password)
        
        # Check if OpenSearch is running
        try:
            print("Testing connection to OpenSearch...")
            info = client.info()
            print(f"Successfully connected to OpenSearch. Version: {info.get('version', {}).get('number', 'unknown')}")
        except Exception as e:
            print(f"Connection error: {e}")
            return
        
        # Check if index exists
        print(f"Checking if index '{args.index}' exists...")
        if not client.indices.exists(index=args.index):
            print(f"Error: Index '{args.index}' does not exist.")
            return
        else:
            print(f"Index '{args.index}' exists.")
        
        # Run benchmark
        print("Starting benchmark run...")
        results = run_benchmark(
            client=client,
            index_name=args.index,
            vector_dimension=args.dimension,
            k=args.k,
            concurrency=args.concurrency,
            duration_seconds=args.duration
        )
        
        print("Benchmark completed successfully.")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
    
    finally:
        print("Script execution completed.")
        sys.stdout.flush()

if __name__ == "__main__":
    main()
