#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''
2. Install the required dependencies:
  bash
   pip install boto3 requests opensearch-py

3. Run the script with the following command:
  bash
   python mongodb_to_opensearch.py \
     --file your_mongodb_export.json \
     --endpoint https://your-opensearch-endpoint.region.es.amazonaws.com \
     --index your-index-name \
     --username your-username \
     --password your-password
'''

import json
import boto3
import requests
from requests.auth import HTTPBasicAuth
import argparse
from datetime import datetime
import os
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.helpers import bulk

def transform_mongodb_json(doc):
    """Transform MongoDB extended JSON format to standard JSON."""
    if isinstance(doc, dict):
        new_doc = {}
        for key, value in doc.items():
            # Skip null values
            if value == "null" or value is None:
                continue

            # Handle MongoDB ObjectId
            if isinstance(value, dict) and "$oid" in value:
                new_doc[key] = value["$oid"]

            # Handle MongoDB Date
            elif isinstance(value, dict) and "$date" in value:
                # Convert ISO date string to timestamp
                date_str = value["$date"]
                try:
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    new_doc[key] = int(dt.timestamp() * 1000)  # milliseconds timestamp
                except:
                    new_doc[key] = date_str

            # Handle MongoDB NumberInt
            elif isinstance(value, dict) and "$numberInt" in value:
                new_doc[key] = int(value["$numberInt"])

            # Handle MongoDB NumberLong
            elif isinstance(value, dict) and "$numberLong" in value:
                new_doc[key] = int(value["$numberLong"])

            # Handle MongoDB NumberDouble
            elif isinstance(value, dict) and "$numberDouble" in value:
                new_doc[key] = float(value["$numberDouble"])

            # Special handling for meta.donate_label.user_label
            elif key == "meta" and isinstance(value, dict) and "donate_label" in value:
                new_doc[key] = transform_mongodb_json(value)
                if "donate_label" in new_doc[key] and "user_label" in new_doc[key]["donate_label"]:
                    user_label = new_doc[key]["donate_label"]["user_label"]
                    if not isinstance(user_label, str):
                        new_doc[key]["donate_label"]["user_label"] = str(user_label)

            # Recursively transform nested objects and arrays
            elif isinstance(value, dict):
                transformed = transform_mongodb_json(value)
                if transformed:  # Only add if not empty
                    new_doc[key] = transformed
            elif isinstance(value, list):
                new_doc[key] = [transform_mongodb_json(item) if isinstance(item, (dict, list)) else item for item in value]
            else:
                new_doc[key] = value
        return new_doc
    elif isinstance(doc, list):
        return [transform_mongodb_json(item) if isinstance(item, (dict, list)) else item for item in doc]
    else:
        return doc

def generate_actions(documents, index_name):
    """Generate bulk actions for OpenSearch."""
    for doc in documents:
        # Use MongoDB ObjectId as document ID if available
        doc_id = None
        
        # Extract MongoDB ObjectId from _id field
        if "_id" in doc:
            if isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
                doc_id = doc["_id"]["$oid"]
            else:
                doc_id = doc["_id"]
            
            # Remove _id field as OpenSearch has its own _id field
            del doc["_id"]
        
        # Ensure we have a document ID
        if not doc_id:
            # Generate a random ID if no MongoDB ObjectId is available
            import uuid
            doc_id = str(uuid.uuid4())
            
        yield {
            "_index": index_name,
            "_id": doc_id,
            "_source": doc
        }

def create_embedding_pipeline(opensearch, pipeline_id, model_id):
    """Create or update an OpenSearch ingestion pipeline for embedding generation."""
    pipeline_config = {
        "description": "Pipeline to generate embeddings for content field",
        "processors": [
            {
                "text_embedding": {
                    "model_id": model_id,
                    "field_map": {
                        "content": "content_embedding"
                    }
                }
            }
        ]
    }
    
    try:
        # Check if pipeline exists
        opensearch.ingest.get_pipeline(id=pipeline_id)
        print(f"Pipeline {pipeline_id} already exists. Updating...")
    except Exception as e:
        print(f"Creating new pipeline {pipeline_id}...")
    
    try:
        # Create or update pipeline
        response = opensearch.ingest.put_pipeline(
            id=pipeline_id,
            body=pipeline_config
        )
        
        if response.get('acknowledged', False):
            print(f"Pipeline {pipeline_id} created/updated successfully")
            return True
        else:
            print(f"Failed to create/update pipeline {pipeline_id}")
            return False
    except Exception as e:
        print(f"Error creating pipeline: {str(e)}")
        return False

def import_to_opensearch(file_path, opensearch_config):
    """Import JSON data from file to OpenSearch."""
    # Read JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Transform MongoDB JSON
    transformed_data = transform_mongodb_json(data)

    # Connect to OpenSearch
    opensearch = OpenSearch(
        hosts=[{'host': opensearch_config['host'], 'port': opensearch_config['port']}],
        http_auth=(opensearch_config['username'], opensearch_config['password']),
        use_ssl=True,
        verify_certs=opensearch_config.get('verify_certs', True),
        connection_class=RequestsHttpConnection
    )

    # Check if index exists, create if not
    if not opensearch.indices.exists(index=opensearch_config['index']):
        try:
            # Define index mapping with vector field configuration
            vector_dimension = opensearch_config.get('vector_dimension', 1024)
            index_mapping = {
                "settings": {
                    "index.knn": True,  # Enable KNN plugin
                    "index.knn.space_type": "l2"  # Use L2 (Euclidean) distance
                },
                "mappings": {
                    "properties": {
                        "content_embedding": {
                            "type": "knn_vector",
                            "dimension": vector_dimension,
                            "method": {
                                "name": "hnsw",
                                "engine": "faiss",
                                "space_type": "l2",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 16
                                }
                            }
                        },
                        "content": {
                            "type": "text"
                        }
                    }
                }
            }
            
            # Create index with vector field configuration
            opensearch.indices.create(
                index=opensearch_config['index'],
                body=index_mapping
            )
            print(f"Created index: {opensearch_config['index']} with vector field configuration")
        except Exception as e:
            print(f"Error creating index with KNN configuration: {str(e)}")
            print("Attempting to create index with basic configuration...")
            
            # Fallback to basic index creation without KNN
            try:
                opensearch.indices.create(index=opensearch_config['index'])
                print(f"Created index: {opensearch_config['index']} with basic configuration")
            except Exception as e2:
                print(f"Failed to create index: {str(e2)}")
                return 0, []
    
    # Create embedding pipeline if specified
    pipeline_id = opensearch_config.get('pipeline_id')
    if pipeline_id and opensearch_config.get('model_id'):
        create_embedding_pipeline(opensearch, pipeline_id, opensearch_config['model_id'])
        print(f"Using pipeline {pipeline_id} for embedding generation")
    
    # Generate actions with pipeline if specified
    actions = list(generate_actions(transformed_data, opensearch_config['index']))
    
    # Add pipeline parameter to actions if pipeline is specified
    if pipeline_id:
        actions = [{**action, "pipeline": pipeline_id} for action in actions]

    # Bulk import
    try:
        success, failed = bulk(
            opensearch,
            actions,
            chunk_size=1000,
            max_retries=3,
            raise_on_error=False
        )

        print(f"Successfully imported {success} documents")
        if failed:
            print(f"Failed to import {len(failed)} documents")

        return success, failed
    except Exception as e:
        print(f"Error during bulk import: {str(e)}")
        return 0, []

def parse_opensearch_endpoint(endpoint):
    """Parse OpenSearch endpoint to extract host and port."""
    if endpoint.startswith('https://'):
        endpoint = endpoint[8:]
    elif endpoint.startswith('http://'):
        endpoint = endpoint[7:]

    if ':' in endpoint:
        host, port = endpoint.split(':')
        return host, int(port)
    else:
        return endpoint, 443  # Default HTTPS port

def main():
    parser = argparse.ArgumentParser(description='Import MongoDB JSON to OpenSearch')
    parser.add_argument('--file', required=True, help='Path to JSON file')
    parser.add_argument('--endpoint', required=True, help='OpenSearch endpoint')
    parser.add_argument('--index', required=True, help='OpenSearch index name')
    parser.add_argument('--username', required=True, help='OpenSearch username')
    parser.add_argument('--password', required=True, help='OpenSearch password')
    parser.add_argument('--verify-certs', action='store_true', default=True, help='Verify SSL certificates')
    parser.add_argument('--pipeline-id', help='OpenSearch ingestion pipeline ID for embedding generation')
    parser.add_argument('--model-id', help='Model ID to use for embedding generation')
    parser.add_argument('--vector-dimension', type=int, default=1024, help='Dimension of the embedding vector (default: 1024)')

    args = parser.parse_args()

    host, port = parse_opensearch_endpoint(args.endpoint)

    opensearch_config = {
        'host': host,
        'port': port,
        'username': args.username,
        'password': args.password,
        'index': args.index,
        'verify_certs': args.verify_certs,
        'vector_dimension': args.vector_dimension
    }
    
    # Add pipeline configuration if provided
    if args.pipeline_id:
        opensearch_config['pipeline_id'] = args.pipeline_id
    if args.model_id:
        opensearch_config['model_id'] = args.model_id

    import_to_opensearch(args.file, opensearch_config)

if __name__ == "__main__":
    main()