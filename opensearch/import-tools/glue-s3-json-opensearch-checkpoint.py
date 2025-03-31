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


import json
import argparse
import io
import boto3
from botocore.exceptions import ClientError
import datetime
import sys
import traceback

from opensearchpy import OpenSearch, helpers
from awsglue.utils import getResolvedOptions

# pip3 install ijson
import ijson  # 用于流式解析 JSON

# 导入检查点功能
from checkpoint_functions import save_checkpoint, load_checkpoint
from document_generators import (
    document_generator_with_checkpoint,
    process_non_array_json_with_checkpoint,
    process_jsonl_format_with_checkpoint
)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'AOS_ENDPOINT',
    'OPENSEARCH_USER',
    'OPENSEARCH_PASSWORD',
    'INDEX',
    'S3_BUCKET',
    'S3_KEY',
    'REGION',
    'IS_ARRAY',
    'FORMAT',
    'ERROR_BUCKET',  # 错误数据存储的S3桶
    'CHECKPOINT_BUCKET',  # 新增参数：存储检查点的S3桶
    'CHECKPOINT_KEY',     # 新增参数：检查点文件的S3键
    'RESUME',             # 新增参数：是否从检查点恢复
    'CHECKPOINT_FREQUENCY' # 新增参数：检查点保存频率（批次数）
])

# 设置默认值
if 'REGION' not in args:
    args['REGION'] = 'us-east-1'
if 'IS_ARRAY' not in args:
    args['IS_ARRAY'] = True
else:
    # 转换字符串为布尔值
    args['IS_ARRAY'] = args['IS_ARRAY'].lower() == 'true'
if 'FORMAT' not in args:
    args['FORMAT'] = 'json'  # 默认为标准JSON格式
if 'ERROR_BUCKET' not in args:
    args['ERROR_BUCKET'] = args['S3_BUCKET']  # 默认使用与源数据相同的桶
if 'CHECKPOINT_BUCKET' not in args:
    args['CHECKPOINT_BUCKET'] = args['S3_BUCKET']  # 默认使用与源数据相同的桶
if 'CHECKPOINT_KEY' not in args:
    # 默认检查点文件路径
    args['CHECKPOINT_KEY'] = f"checkpoints/{args['S3_KEY']}.checkpoint"
if 'RESUME' not in args:
    args['RESUME'] = False
else:
    args['RESUME'] = args['RESUME'].lower() == 'true'
if 'CHECKPOINT_FREQUENCY' not in args:
    args['CHECKPOINT_FREQUENCY'] = 5  # 默认每5个批次保存一次检查点
else:
    args['CHECKPOINT_FREQUENCY'] = int(args['CHECKPOINT_FREQUENCY'])

# 现在可以使用 args 字典访问所有参数
print(f"OpenSearch Endpoint: {args['AOS_ENDPOINT']}")
print(f"Glue Job Name: {args['JOB_NAME']}")

auth = (args['OPENSEARCH_USER'], args['OPENSEARCH_PASSWORD'])

# OpenSearch client configuration
host = args['AOS_ENDPOINT']

opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,
    use_ssl=True,
    verify_certs=False,
    timeout=120,
    http_auth=auth
)

# S3 client configuration
s3_client = boto3.client('s3', region_name=args['REGION'])

# 批量处理大小
BATCH_SIZE = 1000

def get_s3_object_stream(bucket, key):
    """从 S3 获取对象并返回流式对象"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body']
    except ClientError as e:
        print(f"Error accessing S3 object: {e}")
        raise

def transform_mongodb_json(json_data, path=None):
    """
    Transform MongoDB-style JSON by removing type wrappers.
    
    Args:
        json_data: The JSON data to transform (can be dict, list, or primitive)
        path: Current path in the JSON structure (used for special field handling)
        
    Returns:
        Transformed JSON data
    """
    if path is None:
        path = []
        
    # 处理字符串 "null" 的情况，返回 None 以便后续移除该字段
    if json_data == "null":
        return None
    
    # Special handling for meta.data_files field - convert entire content to string
    if '.'.join(path) == 'meta.data_files':
        return json.dumps(json_data)
        
    if isinstance(json_data, dict):
        # Special handling for meta.donate_label.user_label field
        # Convert to string even if it's a MongoDB number type
        if '.'.join(path) == 'meta.donate_label.user_label':
            if len(json_data) == 1:
                key = list(json_data.keys())[0]
                if key in ["$numberInt", "$numberLong", "$numberDouble"]:
                    return str(json_data[key])  # Convert to string instead of number
        
        # Check for MongoDB extended JSON types
        if len(json_data) == 1:
            key = list(json_data.keys())[0]
            if key == "$numberInt":
                return int(json_data[key])
            elif key == "$numberDouble":
                return float(json_data[key])
            elif key == "$numberLong":
                return int(json_data[key])
            elif key == "$date":
                return json_data[key]  # Could convert to datetime if needed
            elif key == "$oid":
                return json_data[key]
        
        # Process regular dictionaries
        result = {}
        for k, v in json_data.items():
            new_path = path + [k]
            transformed_value = transform_mongodb_json(v, new_path)
            # 只有当转换后的值不是 None 时才添加到结果中
            if transformed_value is not None:
                result[k] = transformed_value
        return result
    
    elif isinstance(json_data, list):
        # 过滤掉列表中的 None 值
        transformed_list = [transform_mongodb_json(item, path + [str(i)]) for i, item in enumerate(json_data)]
        return [item for item in transformed_list if item is not None]
    
    else:
        # Return primitive values as is
        return json_data

def extract_mongodb_id(doc):
    """从MongoDB导出的文档中提取ID"""
    if "_id" in doc:
        # 检查是否是MongoDB的ObjectId格式
        if isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
            return doc["_id"]["$oid"]
    return None

def save_failed_batch_to_s3(batch, error_message, bucket):
    """
    将失败的批次数据保存到S3
    
    Args:
        batch: 失败的数据批次
        error_message: 错误信息
        bucket: 目标S3桶
    """
    try:
        # 创建时间戳作为文件名的一部分
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        key = f"error_data/failed_batch_{timestamp}.json"
        
        # 准备要保存的数据，包括错误信息和原始数据
        # 保存文档源数据和ID
        documents_with_ids = []
        for doc in batch:
            document = doc["_source"].copy()  # 复制源数据
            document["_id"] = doc["_id"]      # 添加ID字段
            documents_with_ids.append(document)
            
        data_to_save = {
            "error": error_message,
            "timestamp": datetime.datetime.now().isoformat(),
            "index": args['INDEX'],
            "data": documents_with_ids  # 保存包含_id的完整文档
        }
        
        # 将数据转换为JSON字符串
        json_data = json.dumps(data_to_save, default=str)  # default=str 处理不可序列化的对象
        
        # 上传到S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data,
            ContentType='application/json'
        )
        
        print(f"Failed batch saved to s3://{bucket}/{key}")
        return f"s3://{bucket}/{key}"
    except Exception as e:
        print(f"Error saving failed batch to S3: {e}")
        traceback.print_exc()
        return None

def import_to_opensearch(bucket, key, is_array=True, format='json'):
    """从 S3 导入 JSON 文件到 OpenSearch，支持检查点续读"""
    print(f"Importing data from s3://{bucket}/{key} to OpenSearch index {args['INDEX']}")
    print(f"Using format: {format}")

    # 获取 S3 对象流
    s3_stream = get_s3_object_stream(bucket, key)

    # 检查是否需要从检查点恢复
    start_position = 0
    if args['RESUME']:
        checkpoint_data = load_checkpoint(s3_client, args['CHECKPOINT_BUCKET'], args['CHECKPOINT_KEY'])
        if checkpoint_data:
            start_position = checkpoint_data.get('position', 0)
            print(f"Resuming from position {start_position}")

    # 根据格式选择适当的生成器
    if format.lower() == 'jsonl':
        generator = process_jsonl_format_with_checkpoint
    else:
        # 标准JSON格式
        generator = document_generator_with_checkpoint if is_array else process_non_array_json_with_checkpoint

    # 跟踪进度
    total_docs = 0
    failed_batches = 0
    current_position = start_position

    # 批量导入
    for i, (batch, position) in enumerate(generator(
        s3_stream, 
        transform_mongodb_json, 
        extract_mongodb_id, 
        BATCH_SIZE, 
        args['INDEX'], 
        start_position
    )):
        try:
            success, failed = helpers.bulk(opensearch_client, batch, stats_only=True)
            total_docs += success
            current_position = position  # 更新当前处理位置
            
            print(f"Batch {i+1}: Imported {success} documents, Failed: {failed}")
            print(f"Total documents imported so far: {total_docs}")
            
            # 定期保存检查点
            if (i + 1) % args['CHECKPOINT_FREQUENCY'] == 0:
                checkpoint_data = {
                    'position': current_position,
                    'processed_docs': total_docs,
                    'timestamp': datetime.datetime.now().isoformat()
                }
                save_checkpoint(s3_client, args['CHECKPOINT_BUCKET'], args['CHECKPOINT_KEY'], checkpoint_data)
            
        except Exception as e:
            # 处理异常，保存失败的批次到S3
            error_message = str(e)
            print(f"Error processing batch {i+1}: {error_message}")
            
            # 保存失败的批次到S3
            error_file_path = save_failed_batch_to_s3(batch, error_message, args['ERROR_BUCKET'])
            if error_file_path:
                print(f"Failed batch saved to {error_file_path}")
            
            failed_batches += 1

    # 导入完成后保存最终检查点
    final_checkpoint_data = {
        'position': current_position,
        'processed_docs': total_docs,
        'timestamp': datetime.datetime.now().isoformat(),
        'completed': True
    }
    save_checkpoint(s3_client, args['CHECKPOINT_BUCKET'], args['CHECKPOINT_KEY'], final_checkpoint_data)

    print(f"Import completed. Total documents imported: {total_docs}, Failed batches: {failed_batches}")
    
    if failed_batches > 0:
        print(f"WARNING: {failed_batches} batches failed to import. Check the error_data/ directory in bucket {args['ERROR_BUCKET']} for details.")

if __name__ == "__main__":
    # 执行导入
    import_to_opensearch(args['S3_BUCKET'], args['S3_KEY'], is_array=args['IS_ARRAY'], format=args['FORMAT'])
