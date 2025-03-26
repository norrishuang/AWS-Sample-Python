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

import random
import string
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

def generate_random_record():
    return {
        # 随机生成一个包含 1024 个浮点数（0 到 1 之间）的数组，精度限制为小数点后 6 位
        "my_vector": [round(random.uniform(0, 1), 6) for _ in range(1024)],
        # 随机生成一段连续的英文句子
        "random_string": ' '.join(random.choices(string.ascii_lowercase.split() + ["the", "and", "is", "in", "on", "at", "with", "a", "an"], k=10))
    }

index_name = args.INDEX
bulk_docs = []
total_records = 110000
batch_size = 10000

for i in range(total_records):
    record = generate_random_record()
    bulk_docs.append({
        "_index": index_name,
        "_source": record
    })

    if (i + 1) % batch_size == 0:
        helpers.bulk(opensearch_client, bulk_docs)
        bulk_docs = []
        print(f"Submitted {i + 1} records")

# Submit any remaining documents
if bulk_docs:
    helpers.bulk(opensearch_client, bulk_docs)
    print(f"Submitted remaining {len(bulk_docs)} records")

print("Data import completed")