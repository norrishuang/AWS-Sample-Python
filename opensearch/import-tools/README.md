# S3 to OpenSearch JSON Import Tool

This tool imports JSON data from S3 to OpenSearch, with special handling for MongoDB-style JSON exports.

## Features

- Supports different JSON formats:
  - Standard JSON arrays
  - Non-array JSON objects
  - JSON Lines format (JSONL)
- Handles MongoDB extended JSON format by transforming special type wrappers:
  - `$numberInt` → integer
  - `$numberDouble` → float
  - `$numberLong` → integer
  - `$oid` → string (for document IDs)
- Streams data to minimize memory usage
- Batch processing for efficient imports
- Extracts MongoDB ObjectId for document IDs

## Usage

The script can be run as an AWS Glue job with the following parameters:

```
--JOB_NAME: Name of the Glue job
--AOS_ENDPOINT: OpenSearch endpoint
--OPENSEARCH_USER: OpenSearch username
--OPENSEARCH_PASSWORD: OpenSearch password
--INDEX: Target OpenSearch index
--S3_BUCKET: Source S3 bucket
--S3_KEY: Source S3 object key
--REGION: AWS region (default: us-east-1)
--IS_ARRAY: Whether the JSON is an array (default: true)
--FORMAT: JSON format - 'json' or 'jsonl' (default: json)
--ERROR_BUCKET: S3 bucket for storing failed batches (default: same as S3_BUCKET)
```

## MongoDB JSON Transformation

The script automatically transforms MongoDB-style JSON by removing type wrappers:

```json
// Before transformation
{
  "_id": {"$oid": "674ba7019f7bec469a39b9ac"},
  "height": {"$numberInt": "384"},
  "duration": {"$numberInt": "0"},
  "createdAt": {"$numberDouble": "1.7330112019767473E+09"}
}

// After transformation
{
  "_id": "674ba7019f7bec469a39b9ac",
  "height": 384,
  "duration": 0,
  "createdAt": 1733011201.9767473
}
```

### Special Field Handling

The script includes special handling for certain fields:

- `meta.donate_label.user_label`: This field is always converted to a string type, even if it contains MongoDB number types like `$numberInt` or `$numberLong`.

Example:
```json
// Before transformation
{
  "meta": {
    "donate_label": {
      "user_label": {"$numberInt": "123"}
    }
  }
}

// After transformation
{
  "meta": {
    "donate_label": {
      "user_label": "123"  // Converted to string instead of number
    }
  }
}
```

### Null String Handling

The script removes fields with the string value "null":

```json
// Before transformation
{
  "field1": "value",
  "field2": "null",
  "field3": {
    "nested": "null"
  }
}

// After transformation
{
  "field1": "value",
  "field3": {}  // nested field removed because it was "null"
}
```

This transformation ensures that numeric values are properly indexed in OpenSearch as numbers rather than strings or objects, while special fields are handled according to specific requirements.
