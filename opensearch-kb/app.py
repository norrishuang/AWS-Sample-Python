from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import opensearch_py as opensearch
from sentence_transformers import SentenceTransformer
import numpy as np
from config import *

app = FastAPI(title="OpenSearch Vector Search API")

# 初始化 OpenSearch 客户端
opensearch_client = opensearch.OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)

# 初始化向量模型
model = SentenceTransformer(MODEL_NAME)

class TextInput(BaseModel):
    text: str
    metadata: Optional[dict] = None

class SearchInput(BaseModel):
    query: str
    top_k: int = 5

@app.on_event("startup")
async def startup_event():
    # 创建索引和管道
    create_pipeline()
    create_index()

def create_pipeline():
    pipeline_body = {
        "description": "Text embedding pipeline",
        "processors": [
            {
                "text_embedding": {
                    "model_id": MODEL_NAME,
                    "field_map": {
                        "text": "text_embedding"
                    }
                }
            }
        ]
    }
    
    try:
        opensearch_client.ingest.put_pipeline(
            id=PIPELINE_NAME,
            body=pipeline_body
        )
    except Exception as e:
        print(f"Pipeline creation error: {str(e)}")

def create_index():
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            }
        },
        "mappings": {
            "properties": {
                "text": {"type": "text"},
                "text_embedding": {
                    "type": "knn_vector",
                    "dimension": VECTOR_DIMENSION,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib"
                    }
                },
                "metadata": {"type": "object"}
            }
        }
    }
    
    try:
        if not opensearch_client.indices.exists(INDEX_NAME):
            opensearch_client.indices.create(
                index=INDEX_NAME,
                body=index_body
            )
    except Exception as e:
        print(f"Index creation error: {str(e)}")

@app.post("/ingest")
async def ingest_text(input_data: TextInput):
    try:
        # 生成文本向量
        embedding = model.encode(input_data.text)
        
        # 准备文档
        document = {
            "text": input_data.text,
            "text_embedding": embedding.tolist(),
            "metadata": input_data.metadata or {}
        }
        
        # 存储到 OpenSearch
        response = opensearch_client.index(
            index=INDEX_NAME,
            body=document,
            pipeline=PIPELINE_NAME
        )
        
        return {"status": "success", "id": response["_id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/search")
async def search_similar(input_data: SearchInput):
    try:
        # 生成查询向量
        query_embedding = model.encode(input_data.query)
        
        # 构建向量搜索查询
        search_body = {
            "size": input_data.top_k,
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'text_embedding') + 1.0",
                        "params": {"query_vector": query_embedding.tolist()}
                    }
                }
            }
        }
        
        # 执行搜索
        response = opensearch_client.search(
            index=INDEX_NAME,
            body=search_body
        )
        
        # 处理结果
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "text": hit["_source"]["text"],
                "metadata": hit["_source"].get("metadata", {})
            })
        
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=API_HOST, port=API_PORT) 