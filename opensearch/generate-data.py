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
from faker import Faker
from opensearchpy import OpenSearch

fake = Faker()

def generate_random_data():
    return {
        "goods_id": str(random.randint(800000, 900000)),
        "id": str(random.randint(800000, 900000)),
        "node_ids": [random.randint(100000000, 200000000) for _ in range(3)],
        "first_sale_time_d": fake.date(),
        "mall_promotion_tag_id": str(random.randint(2000000, 3000000)),
        "new_cate_4_id": str(random.randint(10000, 20000)),
        "attribute": [f"{random.randint(1, 1000)}_{random.randint(1, 1000000)}" for _ in range(25)],
        "true_new_flag": str(random.randint(0, 1)),
        "first_shelf_time": fake.date_time().strftime("%Y-%m-%d%H:%M:%S"),
        "cate_ids": [random.randint(4000, 9000) for _ in range(4)],
        "sale_flag": str(random.randint(0, 1)),
        "mall_price": f"{random.uniform(1, 10):.2f}",
        "last_cate_id": str(random.randint(1000, 3000)),
        "sc_url_id": [random.randint(100000, 200000000) for _ in range(30)],
        "mall_status": str(random.randint(0, 1)),
        "mall_code_default": str(random.randint(0, 1)),
        "is_del": str(random.randint(0, 1)),
        "title": fake.word(),
        "site_tp": fake.domain_word(),
        "goods_sn": fake.uuid4(),
        "new_cate_2_id": str(random.randint(3000, 4000)),
        "tag_cloud": [random.randint(30000000, 80000000) for _ in range(100)],
        "fc_id": [random.randint(17000000, 18000000) for _ in range(4)],
        "big_sale_id": [random.randint(100000000, 200000000) for _ in range(7)],
        "ngps_list_recall_score": f"{random.uniform(0, 1):.1f}",
        "real_time_label": [f"layer_group_id_{random.randint(200, 300)}"],
        "mall_short_sale_attrs": f"{random.randint(1, 100)}_{random.randint(1, 1000000)},{random.randint(1, 100)}_{random.randint(1, 100)}-{random.randint(1, 100)}_{random.randint(1, 1000000)}",
        "new_cate_1_id": str(random.randint(2000, 3000)),
        "new_cate_3_id": str(random.randint(5000, 6000)),
        "site_id": fake.country_code().lower(),
        "ngs_list_recall_score": f"{random.uniform(0, 1):.1f}"
    }

# Generate a sample data
sample_data = generate_random_data()
print(sample_data)

# auth = (OPENSEARCH_USER, OPENSEARCH_PASSWORD)
host = "search-mydomain.us-west-1.es.amazonaws.com"
opensearch_client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,  # enables gzip compression for request bodies
    use_ssl=False,
    verify_certs=False,
    # http_auth=auth,
    timeout=120
)