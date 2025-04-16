# OpenSearch Vector Benchmark

这个项目用于生成随机模拟数据并将其导入到OpenSearch中，以便进行向量搜索基准测试。

## 数据结构

生成的数据包含以下字段：
- `content`: 随机生成的英文文本内容（字符串）
- `platform`: 随机选择的平台名称（字符串）
- `date`: 随机生成的日期（日期格式）
- `tag`: 随机生成的标签列表（字符串数组）
- `content_vector`: 1536维的随机浮点数向量（用于向量搜索）

## 向量索引配置

- 算法: HNSW
- 引擎: FAISS
- 维度: 1536
- 空间类型: innerproduct (内积)
- HNSW参数: ef_construction=32, m=8
- 数据类型: FP16 (使用FAISS的SQ编码器)

## 使用方法

### 安装依赖

```bash
pip install opensearch-py numpy faker
```

如果使用AWS IAM认证，还需要安装：
```bash
pip install requests_aws4auth boto3
```

### 连接到Amazon OpenSearch Service

```bash
# 使用基本认证
python opensearch_vector_benchmark.py --num_docs 10000 --host your-domain.region.es.amazonaws.com --port 443 --user username --password password --index vector_benchmark

# 使用AWS IAM认证
python opensearch_vector_benchmark.py --num_docs 10000 --host your-domain.region.es.amazonaws.com --port 443 --aws-auth --region your-region --index vector_benchmark
```

### 连接到自托管OpenSearch

```bash
python opensearch_vector_benchmark.py --num_docs 10000 --host localhost --port 9200 --user admin --password admin --index vector_benchmark
```

## 参数说明

- `--num_docs`: 要生成和索引的文档数量（默认：1000）
- `--host`: OpenSearch主机地址（默认：localhost）
- `--port`: OpenSearch端口（默认：9200，Amazon OpenSearch Service通常为443）
- `--user`: OpenSearch用户名（默认：admin）
- `--password`: OpenSearch密码（默认：admin）
- `--index`: OpenSearch索引名称（默认：vector_benchmark）
- `--aws-auth`: 使用AWS IAM认证而不是基本认证
- `--region`: AWS区域，使用AWS IAM认证时必需（默认：us-east-1）
- `--batch-size`: 每批处理的文档数量（默认：100）

## 注意事项

- 连接到Amazon OpenSearch Service时，请使用完整的域端点作为主机名
- 对于Amazon OpenSearch Service，端口通常为443
- 确保您的IP地址在OpenSearch域的访问策略中被允许
- 生产环境中应启用SSL并使用适当的证书
