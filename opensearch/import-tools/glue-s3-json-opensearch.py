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
    'ERROR_BUCKET'  # 新增参数：错误数据存储的S3桶
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

def document_generator(s3_stream):
    """流式读取 S3 中的 JSON 文件并生成文档"""
    # 处理数组格式的 JSON
    objects = ijson.items(s3_stream, 'item')

    batch = []
    for i, doc in enumerate(objects):
        # 转换MongoDB格式的JSON，去除$numberInt等包装
        doc = transform_mongodb_json(doc)
        
        # 从MongoDB格式中提取ID
        doc_id = extract_mongodb_id(doc)
        if doc_id is None:
            doc_id = doc.get("_id", str(i))  # 使用普通_id或生成新ID
        
        # 创建文档的副本，以便我们可以安全地修改它
        doc_copy = doc.copy()
        
        # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
        if "_id" in doc_copy:
            del doc_copy["_id"]
        
        # 准备文档
        action = {
            "_index": args['INDEX'],
            "_id": doc_id,
            "_source": doc_copy
        }

        batch.append(action)

        # 当批次达到指定大小时，yield 批次并重置
        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []

    # 处理最后一个不完整的批次
    if batch:
        yield batch

def process_non_array_json(s3_stream):
    """处理非数组格式的 JSON 文件"""
    # 使用 ijson 流式解析顶层键
    parser = ijson.parse(s3_stream)

    current_key = None
    current_doc = {}
    doc_count = 0
    batch = []

    for prefix, event, value in parser:
        # 根据 JSON 结构调整此逻辑
        if event == 'map_key':
            current_key = value
        elif prefix.count('.') == 0 and current_key:  # 顶层键值对
            current_doc[current_key] = value
            current_key = None
        elif event == 'end_map' and prefix == '':  # 文档结束
            if current_doc:
                # 转换MongoDB格式的JSON，去除$numberInt等包装
                current_doc = transform_mongodb_json(current_doc)
                
                # 从MongoDB格式中提取ID
                doc_id = extract_mongodb_id(current_doc)
                if doc_id is None:
                    doc_id = current_doc.get("_id", str(doc_count))
                
                # 创建文档的副本，以便我们可以安全地修改它
                doc_copy = current_doc.copy()
                
                # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
                if "_id" in doc_copy:
                    del doc_copy["_id"]
                
                action = {
                    "_index": args['INDEX'],
                    "_id": doc_id,
                    "_source": doc_copy
                }
                batch.append(action)
                doc_count += 1
                current_doc = {}

                if len(batch) >= BATCH_SIZE:
                    yield batch
                    batch = []

    # 处理最后一个批次
    if batch:
        yield batch

def import_to_opensearch(bucket, key, is_array=True, format='json'):
    """从 S3 导入 JSON 文件到 OpenSearch"""
    print(f"Importing data from s3://{bucket}/{key} to OpenSearch index {args['INDEX']}")
    print(f"Using format: {format}")

    # 获取 S3 对象流
    s3_stream = get_s3_object_stream(bucket, key)

    # 根据格式选择适当的生成器
    if format.lower() == 'jsonl':
        generator = process_jsonl_format
    else:
        # 标准JSON格式
        generator = document_generator if is_array else process_non_array_json

    # 跟踪进度
    total_docs = 0
    failed_batches = 0

    # 批量导入
    for i, batch in enumerate(generator(s3_stream)):
        try:
            success, failed = helpers.bulk(opensearch_client, batch, stats_only=True)
            total_docs += success
            print(f"Batch {i+1}: Imported {success} documents, Failed: {failed}")
            print(f"Total documents imported so far: {total_docs}")
            
        except Exception as e:
            # 处理异常，保存失败的批次到S3
            error_message = str(e)
            print(f"Error processing batch {i+1}: {error_message}")
            
            # 保存失败的批次到S3
            error_file_path = save_failed_batch_to_s3(batch, error_message, args['ERROR_BUCKET'])
            if error_file_path:
                print(f"Failed batch saved to {error_file_path}")
            
            failed_batches += 1

    print(f"Import completed. Total documents imported: {total_docs}, Failed batches: {failed_batches}")
    
    if failed_batches > 0:
        print(f"WARNING: {failed_batches} batches failed to import. Check the error_data/ directory in bucket {args['ERROR_BUCKET']} for details.")


def process_jsonl_format(s3_stream):
    """处理JSON Lines格式（每行一个JSON对象）"""
    import json
    import io
    
    batch = []
    doc_count = 0
    
    # 将S3流转换为文本行
    text_stream = io.TextIOWrapper(s3_stream, encoding='utf-8')
    
    # 按行读取
    for line_number, line in enumerate(text_stream, 1):
        if not line.strip():  # 跳过空行
            continue
            
        try:
            # 解析单行JSON
            doc = json.loads(line)
            
            # 转换MongoDB格式的JSON，去除$numberInt等包装
            doc = transform_mongodb_json(doc)
            
            # 从MongoDB格式中提取ID
            doc_id = extract_mongodb_id(doc)
            if doc_id is None:
                doc_id = doc.get("_id", str(doc_count))
                doc_count += 1
            
            # 创建文档的副本，以便我们可以安全地修改它
            doc_copy = doc.copy()
            
            # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
            if "_id" in doc_copy:
                del doc_copy["_id"]
            
            # 准备文档
            action = {
                "_index": args['INDEX'],
                "_id": doc_id,
                "_source": doc_copy
            }
            
            batch.append(action)
            
            # 当批次达到指定大小时，yield 批次并重置
            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []
                
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON at line {line_number}: {e}")
            print(f"Problematic line: {line[:100]}...")  # 只打印前100个字符
            continue  # 跳过这一行，继续处理
    
    # 处理最后一个批次
    if batch:
        yield batch

if __name__ == "__main__":
    # 执行导入
    import_to_opensearch(args['S3_BUCKET'], args['S3_KEY'], is_array=args['IS_ARRAY'], format=args['FORMAT'])
