#!/usr/bin/env python3
"""
Debug script for OpenSearch vector recall issues
"""

import argparse
import numpy as np
from opensearchpy import OpenSearch, RequestsHttpConnection
from sklearn.neighbors import NearestNeighbors

def create_opensearch_client(host, port, username, password, use_ssl=True):
    """Create and return an OpenSearch client."""
    client = OpenSearch(
        hosts=[{'host': host, 'port': int(port)}],
        http_auth=(username, password),
        use_ssl=use_ssl,
        verify_certs=False,
        ssl_show_warn=False,
        connection_class=RequestsHttpConnection,
        timeout=60
    )
    return client

def fetch_vectors(client, index_name, sample_size=100):
    """Fetch document vectors from the index."""
    print(f"Fetching {sample_size} vectors from index {index_name}...")
    
    # Fetch documents with their vectors
    query = {
        "size": sample_size,
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

def build_exact_knn_index(vectors, space_type='innerproduct'):
    """Build an exact KNN index using scikit-learn."""
    print("Building exact KNN index...")
    
    # Normalize vectors if using innerproduct (cosine similarity)
    if space_type.lower() == 'innerproduct':
        # Normalize vectors for cosine similarity
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        # Avoid division by zero
        norms[norms == 0] = 1
        normalized_vectors = vectors / norms
        print("Vectors normalized for innerproduct space type")
        
        # Use cosine similarity (inner product for normalized vectors)
        knn = NearestNeighbors(algorithm='brute', metric='cosine')
        knn.fit(normalized_vectors)
    else:
        # For L2 (Euclidean) space
        knn = NearestNeighbors(algorithm='brute', metric='euclidean')
        knn.fit(vectors)
        print(f"Using Euclidean distance for {space_type} space type")
    
    return knn

def test_recall(client, index_name, doc_ids, vectors, exact_knn, k=10, space_type='innerproduct'):
    """Test recall for a few sample queries."""
    print(f"\nTesting recall with {k} nearest neighbors...")
    
    # Test with a few sample vectors
    num_tests = min(5, len(vectors))
    
    for i in range(num_tests):
        # Use a vector from the dataset as query
        query_idx = i
        query_vector = vectors[query_idx].tolist()
        query_id = doc_ids[query_idx]
        
        print(f"\nTest {i+1}: Using document {query_id} as query")
        
        # Get exact nearest neighbors (ground truth)
        vector_np = np.array(query_vector).reshape(1, -1)
        
        # Normalize the query vector if using innerproduct space
        if space_type.lower() == 'innerproduct':
            norm = np.linalg.norm(vector_np)
            if norm > 0:
                vector_np = vector_np / norm
        
        _, indices = exact_knn.kneighbors(vector_np, n_neighbors=k+1)  # +1 to account for self
        
        # Filter out the query document from ground truth
        ground_truth_indices = [idx for idx in indices[0] if doc_ids[idx] != query_id][:k]
        ground_truth_ids = doc_ids[ground_truth_indices]
        
        print(f"Ground truth IDs: {ground_truth_ids[:5]}... (total: {len(ground_truth_ids)})")
        
        # Query OpenSearch
        query = {
            "size": k + 1,  # +1 to account for the query document itself
            "query": {
                "knn": {
                    "content_vector": {
                        "vector": query_vector,
                        "k": k + 1
                    }
                }
            }
        }
        
        response = client.search(body=query, index=index_name)
        
        # Extract OpenSearch result IDs
        opensearch_ids = [hit['_id'] for hit in response['hits']['hits']]
        
        # Remove the query document from results
        opensearch_ids = [doc_id for doc_id in opensearch_ids if doc_id != query_id][:k]
        
        print(f"OpenSearch IDs: {opensearch_ids[:5]}... (total: {len(opensearch_ids)})")
        
        # Calculate recall
        ground_truth_set = set(ground_truth_ids)
        opensearch_set = set(opensearch_ids)
        
        if not ground_truth_set:
            print("Warning: Empty ground truth set")
            continue
        
        intersection = ground_truth_set.intersection(opensearch_set)
        recall = len(intersection) / len(ground_truth_set)
        
        print(f"Intersection size: {len(intersection)}")
        print(f"Recall: {recall:.4f}")

def main():
    parser = argparse.ArgumentParser(description='Debug OpenSearch vector recall issues')
    parser.add_argument('--host', type=str, default='localhost', help='OpenSearch host')
    parser.add_argument('--port', type=int, default=9200, help='OpenSearch port')
    parser.add_argument('--user', type=str, default='admin', help='OpenSearch username')
    parser.add_argument('--password', type=str, default='admin', help='OpenSearch password')
    parser.add_argument('--index', type=str, default='vector_benchmark', help='OpenSearch index name')
    parser.add_argument('--k', type=int, default=10, help='Number of nearest neighbors')
    parser.add_argument('--sample-size', type=int, default=100, help='Number of documents to sample')
    parser.add_argument('--space-type', type=str, default='innerproduct', help='Space type (innerproduct, l2)')
    
    args = parser.parse_args()
    
    # Create OpenSearch client
    client = create_opensearch_client(args.host, args.port, args.user, args.password)
    
    # Fetch vectors
    doc_ids, vectors = fetch_vectors(client, args.index, args.sample_size)
    
    # Build exact KNN index
    exact_knn = build_exact_knn_index(vectors, args.space_type)
    
    # Test recall
    test_recall(client, args.index, doc_ids, vectors, exact_knn, args.k, args.space_type)

if __name__ == "__main__":
    main()
