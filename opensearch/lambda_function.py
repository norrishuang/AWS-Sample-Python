import boto3
import re
from opensearchpy import OpenSearch, helpers

credentials = boto3.Session().get_credentials()
auth = ('<opensearch-user>', 'opensearch-password')
# the OpenSearch Service domain, e.g. https://search-mydomain.us-west-1.es.amazonaws.com
host = '<opensearch-endpoint>'
index = 'autel-vector-index'
type = '_doc'
url = host + '/' + index + '/' + type

headers = {"Content-Type": "application/json"}

s3 = boto3.client('s3')
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_compress=True,  # enables gzip compression for request bodies
    use_ssl=True,
    verify_certs=False,
    http_auth=auth
)

# Lambda execution starts here
def handler(event, context):
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get, read, and split the file into lines
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        lines = body.splitlines()

        print("start import:" + key)
        print("lines:{rows}".format(rows=len(lines)))
        counts = 0
        # Match the regular expressions to each line and index the JSON
        bulk_docs = []
        for line in lines:

            # 0.func_id,1.sys_cn,2.sys_en,3.func_name_cn,4.func_name_cn_vector,5.func_name_en,6.func_name_en_vector,
            # car_relative_path,func_path_cn,func_path_en,func_des_cn,func_des_cn_vector,func_des_en,func_des_en_vector,func_type
            line = line.decode("utf-8")
            # print("row number:{rownum}".format(rownum=counts))
            cols = line.split(",")
            func_id = cols[0]
            sys_cn = cols[1]
            sys_en = cols[2]
            func_name_cn = cols[3]
            func_name_en = cols[5]
            car_relative_path = cols[7]
            func_path_cn = cols[8]
            func_path_en = cols[9]
            func_des_cn = cols[10]
            func_des_cn_vector = cols[11]
            func_des_en = cols[12]
            func_des_en_vector = cols[13]
            func_type = cols[14]

            # docs = [
            #     { "_index": "words", "_id": "word1", word: "foo" },
            #     { "_index": "words", "_id": "word2", word: "bar" },
            #     { "_index": "words", "_id": "word3", word: "baz" },
            # ]

            document = {"_index": index, "func_id": func_id, "sys_cn": sys_cn, "sys_en": sys_en,
                        "func_name_cn": func_name_cn,
                        "func_name_en": func_name_en,
                        "car_relative_path": car_relative_path,
                        "func_path_cn": func_path_cn,
                        "func_path_en": func_path_en,
                        "func_des_cn_vector": func_des_cn_vector,
                        "func_des_en": func_des_en,
                        "func_des_en_vector": func_des_en_vector,
                        "func_type": func_type}

            if func_name_en == '' or func_name_cn == '':
                counts = counts + 1
                continue

            if counts != 0:
                bulk_docs.append(document)
                # r = requests.post(url, auth=awsauth, json=document, headers=headers)
                # print("request:" + r.text)

            counts = counts + 1
            if counts % 100 == 0:
                print("input: %d" % counts)
                # client.bulk(bulk_docs)
                response = helpers.bulk(client, bulk_docs, max_retries=3)
                print(response)
                bulk_docs = []
                continue
        response = helpers.bulk(client, bulk_docs, max_retries=3)
        print(response)
        print("end import")
