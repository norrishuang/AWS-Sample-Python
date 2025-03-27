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

import sys

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
    'IS_ARRAY'
])

# 设置默认值
if 'REGION' not in args:
    args['REGION'] = 'us-east-1'
if 'IS_ARRAY' not in args:
    args['IS_ARRAY'] = True
else:
    # 转换字符串为布尔值
    args['IS_ARRAY'] = args['IS_ARRAY'].lower() == 'true'

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

def extract_mongodb_id(doc):
    """从MongoDB导出的文档中提取ID"""
    if "_id" in doc:
        # 检查是否是MongoDB的ObjectId格式
        if isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
            return doc["_id"]["$oid"]
    return None

def document_generator(s3_stream):
    """流式读取 S3 中的 JSON 文件并生成文档"""
    # 处理数组格式的 JSON
    objects = ijson.items(s3_stream, 'item')

    batch = []
    for i, doc in enumerate(objects):
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

def import_to_opensearch(bucket, key, is_array=True):
    """从 S3 导入 JSON 文件到 OpenSearch"""
    print(f"Importing data from s3://{bucket}/{key} to OpenSearch index {args['INDEX']}")

    # 获取 S3 对象流
    s3_stream = get_s3_object_stream(bucket, key)

    # 选择适当的生成器
    generator = document_generator if is_array else process_non_array_json

    # 跟踪进度
    total_docs = 0

    # 批量导入
    for i, batch in enumerate(generator(s3_stream)):
        success, failed = helpers.bulk(opensearch_client, batch, stats_only=True)
        total_docs += success
        print(f"Batch {i+1}: Imported {success} documents, Failed: {failed}")
        print(f"Total documents imported so far: {total_docs}")

    print(f"Import completed. Total documents imported: {total_docs}")

if __name__ == "__main__":
    # 执行导入
    import_to_opensearch(args['S3_BUCKET'], args['S3_KEY'], is_array=args['IS_ARRAY'])