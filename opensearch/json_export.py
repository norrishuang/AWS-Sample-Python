import json
import argparse

from opensearchpy import OpenSearch, helpers

# Parse arguments using argparse
parser = argparse.ArgumentParser(description="Generate and upload data to OpenSearch")
parser.add_argument('--AOS_ENDPOINT', required=True, help='The OpenSearch endpoint')
parser.add_argument('--OPENSEARCH_USER', required=True, help='The OpenSearch username')
parser.add_argument('--OPENSEARCH_PASSWORD', required=True, help='The OpenSearch password')
parser.add_argument('--INDEX', required=True, help='The OpenSearch index name')

args = parser.parse_args()

auth = (args.OPENSEARCH_USER, args.OPENSEARCH_PASSWORD)

# OpenSearch client configuration
host = args.AOS_ENDPOINT

opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,
    use_ssl=True,
    verify_certs=False,
    timeout=120,
    http_auth=auth
)

# 读取 MongoDB 导出的 JSON
with open('mongo_export.json', 'r') as f:
    data = json.load(f)

# 如果 JSON 是数组格式，处理它
if isinstance(data, list):
    documents = data
else:
    # 如果是单个文档或其他格式，根据需要调整
    documents = [data]

# 准备批量操作
actions = [
    {
        "_index": args.INDEX,
        "_id": doc.get("_id", None),  # 使用 MongoDB ID 或生成新 ID
        "_source": doc
    }
    for doc in documents
]

# 执行批量导入
helpers.bulk(opensearch_client, actions)