# OpenSearch Vector Benchmark

这个项目用于生成随机模拟数据并将其导入到OpenSearch中，以便进行向量搜索基准测试。

## 数据结构

生成的数据包含以下字段：
- `content`: 随机生成的英文文本内容（字符串）
- `platform`: 随机选择的平台名称（字符串）
- `date`: 随机生成的日期（日期格式）
- `tag`: 随机生成的标签列表（字符串数组）
- `content_vector`: 1536维的随机浮点数向量（用于向量搜索）
- `content_sparse_vector`: 稀疏向量，包含20-40个随机词语及其权重（用于稀疏向量搜索）

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
- `--min-sparse-terms`: 稀疏向量中的最小词语数量（默认：20）
- `--max-sparse-terms`: 稀疏向量中的最大词语数量（默认：40）
- `--shards`: 索引的主分片数量（默认：12）
- `--replicas`: 索引的副本分片数量（默认：1）

## 注意事项

- 连接到Amazon OpenSearch Service时，请使用完整的域端点作为主机名
- 对于Amazon OpenSearch Service，端口通常为443
- 确保您的IP地址在OpenSearch域的访问策略中被允许
- 生产环境中应启用SSL并使用适当的证书


# 并发查询测试脚本(Dense Vector)

## 脚本功能

1. 多线程并发查询：使用ThreadPoolExecutor实现并发查询
2. 随机向量生成：根据指定维度随机生成查询向量
3. 性能指标收集：
   • QPS (每秒查询数)
   • 查询延迟 (P50, P90, P95, P99)
   • 最小/最大/平均延迟
4. 实时进度显示：在测试运行期间显示当前QPS和延迟

## 关键组件

1. LatencyTracker类：跟踪和计算延迟统计数据
2. QueryBenchmark类：执行向量查询并收集性能指标
3. OpenSearch连接：支持基本认证和AWS IAM认证

## 使用方法

```shell
python opensearch_vector_query_benchmark.py [options]
```

### 参数说明

• --host: OpenSearch主机地址（默认：localhost）
• --port: OpenSearch端口（默认：9200）
• --user: OpenSearch用户名（默认：admin）
• --password: OpenSearch密码（默认：admin）
• --index: OpenSearch索引名称（默认：vector_benchmark）
• --aws-auth: 使用AWS IAM认证
• --region: AWS区域（使用AWS IAM认证时必需）
• --dimension: 向量维度（默认：1536）
• --concurrency: 并发查询线程数（默认：10）
• --duration: 测试持续时间（秒，默认：60）
• --k: 检索的最近邻数量（默认：10）

### 连接到Amazon OpenSearch Service示例

bash
# 使用基本认证
```shell
python opensearch_vector_query_benchmark.py --host your-domain.region.es.amazonaws.com --port 443 --user username --password password --concurrency 20 --duration 120
```

# 使用AWS IAM认证
```shell
python opensearch_vector_query_benchmark.py --host your-domain.region.es.amazonaws.com --port 443 --aws-auth --region your-region --concurrency 20 --duration 120
```

### 连接到自托管OpenSearch示例

```shell
python opensearch_vector_query_benchmark.py --host localhost --port 9200 --user admin --password admin --dimension 1536 --concurrency 20 --duration 60
```

## 输出结果

脚本运行完成后，将显示详细的性能指标报告，包括：

• 总查询次数
• 测试持续时间
• QPS（每秒查询数）
• 延迟统计（毫秒）：
• 最小延迟
• 平均延迟
• P50（中位数）延迟
• P90延迟
• P95延迟
• P99延迟
• 最大延迟

## 注意事项

1. 在运行此脚本之前，请确保已经使用 opensearch_vector_benchmark.py 创建了索引并导入了数据
2. 对于Amazon OpenSearch Service，请使用完整的域端点作为主机名，端口通常为443
3. 确保您的IP地址在OpenSearch域的访问策略中被允许
4. 根据您的环境调整并发数和测试持续时间

这个脚本将帮助您评估OpenSearch向量搜索的性能，并提供详细的延迟指标，以便您可以优化您的向量搜索应用程序。


# 并发查询测试脚本(Sparse Vector)

## 脚本功能特点

1. 稀疏向量查询生成：
   • 从索引中采样文档，提取真实的稀疏向量词条
   • 随机选择 3-8 个词条（可配置）构建查询
   • 随机使用不同的 rank_feature 函数（saturation、log、sigmoid）

2. 多进程并发测试：
   • 使用 Python 的 multiprocessing 模块实现真正的并行查询
   • 可配置并发进程数量

3. 性能指标收集：
   • QPS (每秒查询数)
   • 查询延迟统计 (最小、最大、平均、P50、P90、P95、P99)
   • 实时进度显示

## 使用方法

bash
# 基本用法
```shell
python opensearch_sparse_vector_query_benchmark.py --host localhost --port 9200 --user admin --password admin
```

# 连接到 Amazon OpenSearch Service (使用基本认证)
```shell
python opensearch_sparse_vector_query_benchmark.py --host your-domain.region.es.amazonaws.com --port 443 --user username --password password
```

# 连接到 Amazon OpenSearch Service (使用 AWS IAM 认证)
```shell
python opensearch_sparse_vector_query_benchmark.py --host your-domain.region.es.amazonaws.com --port 443 --aws-auth --region your-region
```

# 自定义测试参数
```shell
python opensearch_sparse_vector_query_benchmark.py --concurrency 20 --duration 120 --min-terms 5 --max-terms 10
```

## 参数说明

• --host: OpenSearch 主机地址（默认：localhost）
• --port: OpenSearch 端口（默认：9200）
• --user: OpenSearch 用户名（默认：admin）
• --password: OpenSearch 密码（默认：admin）
• --index: OpenSearch 索引名称（默认：vector_benchmark）
• --aws-auth: 使用 AWS IAM 认证
• --region: AWS 区域（使用 AWS IAM 认证时必需）
• --concurrency: 并发查询进程数（默认：4）
• --duration: 测试持续时间（秒，默认：60）
• --k: 检索的结果数量（默认：10）
• --min-terms: 每个查询中的最小词条数（默认：3）
• --max-terms: 每个查询中的最大词条数（默认：8）
• --sample-size: 用于提取词条的文档采样数量（默认：100）

## 查询生成机制

脚本会从索引中采样文档，提取真实的稀疏向量词条，然后随机选择词条构建查询。每个查询会使用 bool.should 组合多个 rank_feature 查询，并随机选择不同的 rank_feature 函数（saturation、log、sigmoid）以
模拟真实的查询场景。

这个脚本将帮助你评估 OpenSearch 稀疏向量搜索的性能，并提供详细的延迟指标，以便你可以优化你的稀疏向量搜索应用程序。