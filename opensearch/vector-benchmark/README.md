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
- 数据类型: FP16 (float16)

## 使用方法

### 安装依赖

```bash
pip install opensearch-py numpy faker
```

### 运行脚本

```bash
python opensearch_vector_benchmark.py --num_docs 10000 --host localhost --port 9200 --user admin --password admin --index vector_benchmark
```

参数说明：
- `--num_docs`: 要生成和索引的文档数量（默认：1000）
- `--host`: OpenSearch主机地址（默认：localhost）
- `--port`: OpenSearch端口（默认：9200）
- `--user`: OpenSearch用户名（默认：admin）
- `--password`: OpenSearch密码（默认：admin）
- `--index`: OpenSearch索引名称（默认：vector_benchmark）

## 注意事项

- 默认连接到本地OpenSearch实例（localhost:9200）
- 默认使用基本认证（admin/admin）
- 生产环境中应启用SSL并使用适当的证书
