# S3 to OpenSearch JSON Import Tool with Checkpoint Support

This enhanced version of the tool imports JSON data from S3 to OpenSearch with checkpoint support, allowing for resumption of interrupted imports.

## New Features

- **Checkpoint Support**: Saves progress periodically and can resume from the last saved position
- **Efficient Processing**: Skips already processed records when resuming
- **Configurable Checkpoint Frequency**: Control how often checkpoints are saved

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
--CHECKPOINT_BUCKET: S3 bucket for storing checkpoint data (default: same as S3_BUCKET)
--CHECKPOINT_KEY: S3 key for checkpoint file (default: checkpoints/{S3_KEY}.checkpoint)
--RESUME: Whether to resume from checkpoint (default: false)
--CHECKPOINT_FREQUENCY: How often to save checkpoints (default: every 5 batches)
```

## How Checkpointing Works

1. The script tracks the current position in the source file
2. Checkpoints are saved periodically to S3 (controlled by CHECKPOINT_FREQUENCY)
3. When resuming, the script loads the checkpoint and skips already processed records
4. A final checkpoint is saved when processing completes

## Example Usage

### Initial Run:

```
--JOB_NAME=import-json-to-opensearch
--AOS_ENDPOINT=your-opensearch-endpoint
--OPENSEARCH_USER=your-username
--OPENSEARCH_PASSWORD=your-password
--INDEX=your-index
--S3_BUCKET=your-bucket
--S3_KEY=path/to/data.json
--REGION=us-east-1
--IS_ARRAY=true
--FORMAT=json
--CHECKPOINT_FREQUENCY=10
```

### Resume After Interruption:

```
--JOB_NAME=import-json-to-opensearch
--AOS_ENDPOINT=your-opensearch-endpoint
--OPENSEARCH_USER=your-username
--OPENSEARCH_PASSWORD=your-password
--INDEX=your-index
--S3_BUCKET=your-bucket
--S3_KEY=path/to/data.json
--REGION=us-east-1
--IS_ARRAY=true
--FORMAT=json
--RESUME=true
--CHECKPOINT_FREQUENCY=10
```

## Implementation Details

The checkpoint functionality is implemented through:

1. `checkpoint_functions.py`: Contains functions for saving and loading checkpoints
2. `document_generators.py`: Modified generators that support starting from a specific position
3. `glue-s3-json-opensearch-checkpoint.py`: Main script with checkpoint integration

Each checkpoint contains:
- Current position in the file
- Number of documents processed
- Timestamp
- Completion status
