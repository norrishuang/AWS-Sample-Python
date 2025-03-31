import json
import argparse

from opensearchpy import OpenSearch, helpers

# pip3 install ijson
import ijson  # 用于流式解析 JSON

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

# 批量处理大小
BATCH_SIZE = 1000

def document_generator(filename):
    """流式读取 JSON 文件并生成文档"""
    # 处理数组格式的 JSON
    with open(filename, 'rb') as f:
        # 如果 JSON 是顶层数组格式 [doc1, doc2, ...]
        objects = ijson.items(f, 'item')
        
        batch = []
        for i, doc in enumerate(objects):
            # 准备文档
            action = {
                "_index": args.INDEX,
                "_id": doc.get("_id", str(i)),  # 使用文档ID或生成新ID
                "_source": doc
            }
            
            batch.append(action)
            
            # 当批次达到指定大小时，yield 批次并重置
            if len(batch) >= BATCH_SIZE:
                yield batch
                batch = []
        
        # 处理最后一个不完整的批次
        if batch:
            yield batch

def process_non_array_json(filename):
    """处理非数组格式的 JSON 文件"""
    with open(filename, 'rb') as f:
        # 使用 ijson 流式解析顶层键
        parser = ijson.parse(f)
        
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
                    action = {
                        "_index": args.INDEX,
                        "_id": current_doc.get("_id", str(doc_count)),
                        "_source": current_doc
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

def import_to_opensearch(filename, is_array=True):
    """导入 JSON 文件到 OpenSearch"""
    # 选择适当的生成器
    generator = document_generator if is_array else process_non_array_json
    
    # 跟踪进度
    total_docs = 0
    
    # 批量导入
    for i, batch in enumerate(generator(filename)):
        success, failed = helpers.bulk(opensearch_client, batch, stats_only=True)
        total_docs += success
        print(f"Batch {i+1}: Imported {success} documents, Failed: {failed}")
        print(f"Total documents imported so far: {total_docs}")

# 执行导入
import_to_opensearch('/home/ec2-user/environment/anker/mongo_export.json', is_array=True)  # 根据 JSON 格式调整