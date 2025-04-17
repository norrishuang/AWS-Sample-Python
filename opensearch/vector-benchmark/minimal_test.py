#!/usr/bin/env python3
"""
Minimal OpenSearch Vector Query Test
"""

import argparse
import numpy as np
import time
from opensearchpy import OpenSearch, RequestsHttpConnection

def main():
    parser = argparse.ArgumentParser(description='Minimal OpenSearch Vector Search Test')
    parser.add_argument('--host', type=str, required=True, help='OpenSearch host')
    parser.add_argument('--port', type=int, required=True, help='OpenSearch port')
    parser.add_argument('--user', type=str, required=True, help='OpenSearch username')
    parser.add_argument('--password', type=str, required=True, help='OpenSearch password')
    parser.add_argument('--index', type=str, required=True, help='OpenSearch index name')
    parser.add_argument('--dimension', type=int, default=1536, help='Vector dimension')
    
    args = parser.parse_args()
    
    # Create OpenSearch client
    print(f"Connecting to OpenSearch at {args.host}:{args.port}...")
    client = OpenSearch(
        hosts=[{'host': args.host, 'port': int(args.port)}],
        http_auth=(args.user, args.password),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=60
    )
    
    # Check connection
    try:
        info = client.info()
        print(f"Successfully connected to OpenSearch. Version: {info.get('version', {}).get('number', 'unknown')}")
    except Exception as e:
        print(f"Connection error: {e}")
        return
    
    # Check if index exists
    if not client.indices.exists(index=args.index):
        print(f"Error: Index '{args.index}' does not exist.")
        return
    else:
        print(f"Index '{args.index}' exists.")
    
    # Get document count
    try:
        count = client.count(index=args.index)['count']
        print(f"Index contains {count} documents.")
    except Exception as e:
        print(f"Error getting document count: {e}")
    
    # Generate a random vector
    print("Generating random vector...")
    vector = np.random.uniform(-1, 1, args.dimension).tolist()
    
    # Perform a single vector search
    print("Performing vector search...")
    query = {
        "size": 10,
        "query": {
            "knn": {
                "content_vector": {
                    "vector": vector,
                    "k": 10
                }
            }
        }
    }
    
    try:
        start_time = time.time()
        response = client.search(body=query, index=args.index)
        end_time = time.time()
        
        took_ms = response.get('took', 0)
        hit_count = len(response['hits']['hits'])
        
        print(f"Search completed in {took_ms} ms (OpenSearch reported time)")
        print(f"Total round-trip time: {(end_time - start_time) * 1000:.2f} ms")
        print(f"Found {hit_count} results")
        
        # Print the first result
        if hit_count > 0:
            first_hit = response['hits']['hits'][0]
            print(f"First hit score: {first_hit['_score']}")
            print(f"First hit ID: {first_hit['_id']}")
            
    except Exception as e:
        print(f"Search error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
    
    print("Test completed.")

if __name__ == "__main__":
    main()
