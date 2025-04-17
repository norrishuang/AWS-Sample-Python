#!/usr/bin/env python3
"""
Simple OpenSearch Vector Query Benchmark

A simplified version that focuses only on running the benchmark and displaying results.
"""

import argparse
import numpy as np
import time
import statistics
from collections import deque
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from opensearchpy import OpenSearch, RequestsHttpConnection

# Default settings
OPENSEARCH_HOST = 'localhost'
OPENSEARCH_PORT = 9200
OPENSEARCH_USER = 'admin'
OPENSEARCH_PASSWORD = 'admin'
INDEX_NAME = 'vector_benchmark'
VECTOR_DIMENSION = 1536
DEFAULT_CONCURRENCY = 10
DEFAULT_DURATION = 60
DEFAULT_K = 10

class LatencyTracker:
    def __init__(self):
        self.latencies = []
        self.lock = threading.Lock()
    
    def add_latency(self, latency_ms):
        with self.lock:
            self.latencies.append(latency_ms)
    
    def get_percentile(self, percentile):
        with self.lock:
            if not self.latencies:
                return 0
            sorted_latencies = sorted(self.latencies)
            idx = int(len(sorted_latencies) * percentile / 100)
            return sorted_latencies[idx]
    
    def get_stats(self):
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
    client = OpenSearch(
        hosts=[{'host': host, 'port': int(port)}],
        http_auth=(username, password),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=60
    )
    return client

def run_benchmark(client, index_name, vector_dimension, k, concurrency, duration_seconds):
    # Setup
    latency_tracker = LatencyTracker()
    query_count = 0
    query_count_lock = threading.Lock()
    running = True
    
    def generate_random_vector():
        return np.random.uniform(-1, 1, vector_dimension).tolist()
    
    def perform_vector_search():
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
        while running:
            perform_vector_search()
    
    # Start benchmark
    print(f"Starting benchmark with {concurrency} concurrent threads for {duration_seconds} seconds...")
    start_time = time.time()
    
    # Create and start worker threads
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        
        # Monitor and report progress
        try:
            elapsed = 0
            while elapsed < duration_seconds:
                time.sleep(1)
                elapsed = time.time() - start_time
                current_qps = query_count / elapsed if elapsed > 0 else 0
                stats = latency_tracker.get_stats()
                
                # Print progress update
                sys.stdout.write(f"\rRunning: {elapsed:.1f}s | "
                                f"Queries: {query_count} | "
                                f"QPS: {current_qps:.1f} | "
                                f"P50: {stats['p50']:.1f}ms | "
                                f"P99: {stats['p99']:.1f}ms")
                sys.stdout.flush()
        
        finally:
            # Stop the benchmark
            running = False
            end_time = time.time()
    
    # Calculate final results
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
    parser = argparse.ArgumentParser(description='Simple OpenSearch Vector Search Benchmark')
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
        # Create OpenSearch client
        print(f"Connecting to OpenSearch at {args.host}:{args.port}...")
        client = create_opensearch_client(args.host, args.port, args.user, args.password)
        
        # Check if OpenSearch is running
        try:
            info = client.info()
            print(f"Successfully connected to OpenSearch.")
        except Exception as e:
            print(f"Connection error: {e}")
            return
        
        # Check if index exists
        if not client.indices.exists(index=args.index):
            print(f"Error: Index '{args.index}' does not exist.")
            return
        
        # Run benchmark
        run_benchmark(
            client=client,
            index_name=args.index,
            vector_dimension=args.dimension,
            k=args.k,
            concurrency=args.concurrency,
            duration_seconds=args.duration
        )
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    main()
