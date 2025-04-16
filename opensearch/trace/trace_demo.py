#!/usr/bin/env python3
"""
OpenSearch Trace Demo - 模拟生成 trace 数据并发送到 OpenSearch Ingestion Pipeline
"""

import time
import random
import uuid
import json
import requests
import boto3
from datetime import datetime
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from aws_requests_auth.aws_auth import AWSRequestsAuth

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.requests import RequestsInstrumentor

# OpenSearch Ingestion Pipeline URL
INGESTION_URL = "https://trace-logs-demo-wwxfoyslntxmqkwjzgnftufjq4.us-east-1.osis.amazonaws.com"

# AWS 区域
AWS_REGION = "us-east-1"  # 根据你的 OpenSearch 服务所在区域调整

# 模拟的服务名称和版本
SERVICE_NAME = "demo-web-service"
SERVICE_VERSION = "1.0.0"

# 模拟的 API 端点
API_ENDPOINTS = [
    "/api/users",
    "/api/products",
    "/api/orders",
    "/api/checkout",
    "/api/search"
]

# 模拟的 HTTP 方法
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]

# 模拟的 HTTP 状态码
HTTP_STATUS_CODES = [200, 201, 400, 401, 403, 404, 500]
HTTP_STATUS_WEIGHTS = [0.7, 0.1, 0.05, 0.05, 0.03, 0.05, 0.02]  # 权重，大部分请求是成功的

# 初始化 OpenTelemetry
def setup_opentelemetry():
    resource = Resource.create({
        "service.name": SERVICE_NAME,
        "service.version": SERVICE_VERSION,
        "environment": "production"
    })
    
    trace.set_tracer_provider(TracerProvider(resource=resource))
    
    # 创建 AWS 认证
    session = boto3.Session()
    credentials = session.get_credentials()
    
    # 配置 OTLP exporter，将数据发送到 OpenSearch Ingestion Pipeline
    # 由于 OTLPSpanExporter 不支持自定义 HTTP 客户端，我们将使用一个简单的方法
    # 先使用标准的 exporter，然后在主程序中手动发送数据
    
    otlp_exporter = OTLPSpanExporter(
        endpoint=f"{INGESTION_URL}/v1/traces"
    )
    
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    # 自动检测 requests 库的调用
    RequestsInstrumentor().instrument()
    
    return trace.get_tracer(__name__)

# 模拟用户 ID
def generate_user_id():
    return f"user_{random.randint(1, 1000)}"

# 模拟 IP 地址
def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

# 模拟一个 HTTP 请求
def simulate_http_request(tracer):
    user_id = generate_user_id()
    client_ip = generate_ip()
    endpoint = random.choice(API_ENDPOINTS)
    method = random.choice(HTTP_METHODS)
    status_code = random.choices(HTTP_STATUS_CODES, weights=HTTP_STATUS_WEIGHTS)[0]
    
    # 创建一个根 span 表示整个 HTTP 请求
    with tracer.start_as_current_span(
        name=f"{method} {endpoint}",
        attributes={
            "http.method": method,
            "http.url": f"https://example.com{endpoint}",
            "http.status_code": status_code,
            "http.client_ip": client_ip,
            "user.id": user_id
        }
    ) as parent_span:
        # 记录请求开始时间
        start_time = time.time()
        
        # 模拟请求处理时间
        processing_time = random.uniform(0.01, 0.5)
        time.sleep(processing_time)
        
        # 如果是 GET /api/products，模拟数据库查询
        if method == "GET" and endpoint == "/api/products":
            with tracer.start_as_current_span(
                name="database.query",
                attributes={
                    "db.system": "postgresql",
                    "db.name": "products_db",
                    "db.operation": "SELECT"
                }
            ):
                # 模拟数据库查询时间
                db_query_time = random.uniform(0.05, 0.2)
                time.sleep(db_query_time)
        
        # 如果是 POST /api/orders，模拟创建订单和支付处理
        if method == "POST" and endpoint == "/api/orders":
            # 订单创建子 span
            with tracer.start_as_current_span(
                name="create_order",
                attributes={
                    "order.id": str(uuid.uuid4()),
                    "order.amount": round(random.uniform(10.0, 500.0), 2)
                }
            ):
                time.sleep(random.uniform(0.03, 0.1))
            
            # 支付处理子 span
            with tracer.start_as_current_span(
                name="process_payment",
                attributes={
                    "payment.provider": random.choice(["stripe", "paypal", "credit_card"]),
                    "payment.status": random.choice(["success", "success", "success", "failed"])
                }
            ):
                time.sleep(random.uniform(0.1, 0.3))
        
        # 计算总处理时间
        total_time = time.time() - start_time
        parent_span.set_attribute("http.request_duration_ms", total_time * 1000)
        
        # 如果状态码是错误，添加错误信息
        if status_code >= 400:
            parent_span.set_status(trace.Status(trace.StatusCode.ERROR))
            if status_code == 404:
                parent_span.record_exception(Exception("Resource not found"))
            elif status_code == 500:
                parent_span.record_exception(Exception("Internal server error"))
            else:
                parent_span.record_exception(Exception(f"HTTP error {status_code}"))

def main():
    print(f"由于 OpenTelemetry 的认证问题，请使用 trace_demo_simple.py 脚本")
    print(f"运行命令: python trace_demo_simple.py")
    
    print(f"\n如果你仍然想尝试使用 OpenTelemetry，请参考以下资源:")
    print(f"- https://opentelemetry-python.readthedocs.io/en/latest/exporter/otlp/otlp.html")
    print(f"- https://github.com/open-telemetry/opentelemetry-python/issues/2751")
    
    print(f"\n推荐使用 trace_demo_simple.py 或 trace_demo_alternative.py")
    
    return

if __name__ == "__main__":
    main()
