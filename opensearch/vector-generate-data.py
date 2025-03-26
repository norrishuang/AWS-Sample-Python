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
import sys

from awsglue.utils import getResolvedOptions
from opensearchpy import OpenSearch, helpers

args = getResolvedOptions(sys.argv, ['AOS_ENDPOINT','OPENSEARCH_USER','OPENSEARCH_PASSWORD','INDEX'])


auth = (args['OPENSEARCH_USER'], args['OPENSEARCH_PASSWORD'])

# OpenSearch client configuration
host = args['AOS_ENDPOINT']
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,
    use_ssl=False,
    verify_certs=False,
    timeout=120,
    http_auth=auth
)

def generate_random_record():
    return {
        "my_vector": [random.randint(0, 1000000) for _ in range(1024)],
        "random_string": ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    }

index_name = args['INDEX']
bulk_docs = []
total_records = 10000
batch_size = 1000

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