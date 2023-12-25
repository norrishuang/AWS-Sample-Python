#!/bin/sh

## 通过 elastidump 向 opensearch 导入样本
## 输入参数：
##     1. 执行号
##     2. opensearch 用户名
##     3. opensearch 密码
##     4. opensearch endpoint
## 这里使用的 sample data 是通过 datafaker 生成，后再添加 dump opensearch 的 json 格式，如下样例，[对于 elasticsearch 6.8，不需要指定 "_type":"_doc"]
## {"_index":"logdatatest","_source":{"sentence": "\u7535\u5f71\u6211\u7684\u7c7b\u522b\u51fa\u6765\u53c2\u52a0\u6700\u540e.", "locale": "cy_GB", "file_name": "\u4e0a\u6d77.mp3", "datetime": "2002-01-08 20:
   #51:05", "uuid4": "0aa7b54e-3875-40d5-8611-dbb15f81cb73", "timezone": "America/Cambridge_Bay", "company_email": "jing46@66.cn", "domain_name": "qiangding.cn", "credit_card_number": "6011465354263476", "ip
   #v4": "198.16.110.165", "linux_platform_token": "X11; Linux x86_64", "mac_address": "c1:91:79:6e:d1:0c", "latitude": 9845, "user_name": "xiaogang", "phone_number": "13254447989", "company": "\u4e1c\u65b9\
   #u5cfb\u666f\u7f51\u7edc\u6709\u9650\u516c\u53f8", "job": "\u4e2d\u56fd\u65b9\u8a00\u7ffb\u8bd1", "address": "\u5c71\u897f\u7701\u5065\u5e02\u9ad8\u660e\u5218\u8857K\u5ea7 838327", "md5": "7f0ac2d7255f497
   #53f319d631cfd9316", "word": "\u8ba4\u4e3a", "isbn10": "1-9835-1521-3", "url": "http://www.juanjiang.cn/", "uri": "http://www.73.cn/main/faq.html", "longitude": 5006, "image_url": "https://dummyimage.com/
   #941x1001", "company_suffix": "\u79d1\u6280\u6709\u9650\u516c\u53f8", "street_address": "\u516d\u76d8\u6c34\u8857z\u5ea7"}}


EXEC_NO=$1
USER_NAME=$2
USER_PASSWORD=$3
OPENSEARCH_ENDPOINT=$4

echo "input 100 files with N.01,10W records pre file. Execute No." $EXEC_N

#OPENSEARCH_ENDPOINT=vpc-migrate-6-8-kj4jfvx7jrpg7lbj3fujcxkwdm.us-east-1.es.amazonaws.com


echo "input 100 files with N.01,10W records pre file. Execute No." $EXEC_NO
for  ((i=1;i<=100;i++)) do
  echo "dump file to cmblogtest_10cols_a-index,file nums:" $i
  elasticdump \
    --input="./data/es_data_small_cols_10w_01.json" \
    --output=https://"$USER_NAME":"$USER_PASSWORD"@"$OPENSEARCH_ENDPOINT"/logs-index/ \
    --limit=10000 \
    --type=data >> log/elasticdump_log_"$EXEC_NO".log
    # --transform="doc._source=Object.assign({},doc)" >> log/elasticdump_log_"$EXEC_NO"_"$FILE_NO".log
  sleep 2
done

echo "Done, Execute No." $EXEC_NO