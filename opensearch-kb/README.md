# OpenSearch 向量搜索服务

这是一个基于 FastAPI 的 OpenSearch 向量搜索服务，支持文本向量化和相似度搜索功能。该服务可以与 Dify 外部知识库集成。

## 功能特点

- 文本向量化存储
- 向量相似度搜索
- 支持元数据存储
- 与 Dify 外部知识库集成

## 环境要求

- Python 3.8+
- OpenSearch 2.x
- 足够的系统内存用于运行向量模型

## 安装

1. 克隆项目并安装依赖：

```bash
cd opensearch-kb
pip install -r requirements.txt
```

2. 配置环境变量：

创建 `.env` 文件并设置以下环境变量：

```env
OPENSEARCH_HOST=your_opensearch_host
OPENSEARCH_PORT=9200
OPENSEARCH_USER=your_username
OPENSEARCH_PASSWORD=your_password
API_HOST=0.0.0.0
API_PORT=8000
```

## 运行服务

```bash
python app.py
```

服务将在 http://localhost:8000 启动

## API 接口

### 1. 文本向量化存储

```http
POST /ingest
Content-Type: application/json

{
    "text": "要存储的文本内容",
    "metadata": {
        "source": "文档来源",
        "timestamp": "2024-01-01"
    }
}
```

### 2. 向量相似度搜索

```http
POST /search
Content-Type: application/json

{
    "query": "搜索查询文本",
    "top_k": 5
}
```

## 与 Dify 集成

1. 在 Dify 中创建外部知识库
2. 配置 API 端点：
   - 向量化接口：`http://your-server:8000/ingest`
   - 搜索接口：`http://your-server:8000/search`

## 项目结构

```
opensearch-kb/
├── app.py              # FastAPI 应用主文件
├── config.py           # 配置文件
├── requirements.txt    # 项目依赖
└── README.md          # 项目说明文档
```

## 注意事项

- 确保 OpenSearch 集群已启用向量搜索功能
- 建议在生产环境中使用 HTTPS
- 根据实际需求调整向量模型的参数和配置
- 默认使用 `sentence-transformers/all-MiniLM-L6-v2` 模型，可以在 `config.py` 中修改 