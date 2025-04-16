# OpenSearch Vector Benchmark with Sparse Vectors

This project has been enhanced to support both dense and sparse vector embeddings for benchmarking OpenSearch vector search capabilities.

## Added Features

- Support for sparse vector field (`content_sparse_vector`) in the OpenSearch index using the `rank_features` field type
- Random sparse vector generation with configurable term count (20-40 terms by default)
- Command line parameters to customize sparse vector generation

## Sparse Vector Implementation

The sparse vector implementation:
- Generates a random vocabulary of terms
- Randomly selects between 20-40 terms (configurable)
- Assigns random weights between 0.1 and 1.0 to each term
- Stores the sparse vector as a dictionary where keys are terms and values are weights
- Uses OpenSearch's `rank_features` field type for efficient sparse vector operations

## Usage Example

```bash
# Basic usage with default sparse vector settings (20-40 terms)
python opensearch_vector_benchmark.py --num_docs 1000 --host localhost --port 9200

# Customize sparse vector term count
python opensearch_vector_benchmark.py --num_docs 1000 --min-sparse-terms 10 --max-sparse-terms 50
```

## OpenSearch Sparse Vector Search Example

After indexing data, you can perform sparse vector searches like:

```json
GET vector_benchmark/_search
{
  "query": {
    "rank_feature": {
      "content_sparse_vector.term1": {
        "saturation": {}
      }
    }
  }
}
```

Or use a more complex query with multiple terms:

```json
GET vector_benchmark/_search
{
  "query": {
    "bool": {
      "should": [
        {
          "rank_feature": {
            "content_sparse_vector.term1": {
              "saturation": {}
            }
          }
        },
        {
          "rank_feature": {
            "content_sparse_vector.term2": {
              "saturation": {}
            }
          }
        }
      ]
    }
  }
}
```

This enhancement allows for benchmarking both dense and sparse vector search capabilities in OpenSearch.
