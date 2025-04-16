#!/usr/bin/env python3
"""
OpenSearch Trace Demo - 使用直接 HTTP 请求发送 trace 数据到 OpenSearch Ingestion Pipeline
"""

import time
import random
import uuid
import json
import requests
import boto3
from datetime import datetime
from aws_requests_auth.aws_auth import AWSRequestsAuth

# OpenSearch Ingestion Pipeline URL
INGESTION_URL = "https://trace-analysis-pipeline-prs2hjjormceysqqvbud6r6eyu.us-east-1.osis.amazonaws.com"

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

# 创建 AWS SigV4 认证
def create_aws_auth():
    session = boto3.Session()
    credentials = session.get_credentials()
    
    return AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_token=credentials.token,
        aws_host=INGESTION_URL.replace("https://", ""),
        aws_region=AWS_REGION,
        aws_service="osis"
    )

# 生成随机 ID
def generate_id():
    return str(uuid.uuid4())

# 模拟用户 ID
def generate_user_id():
    return f"user_{random.randint(1, 1000)}"

# 模拟 IP 地址
def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

# 生成当前时间戳（纳秒）
def current_time_nanos():
    return int(time.time() * 1_000_000_000)

# 创建一个 trace
def create_trace():
    trace_id = generate_id().replace("-", "")
    
    # 选择随机的请求参数
    user_id = generate_user_id()
    client_ip = generate_ip()
    endpoint = random.choice(API_ENDPOINTS)
    method = random.choice(HTTP_METHODS)
    status_code = random.choices(HTTP_STATUS_CODES, weights=HTTP_STATUS_WEIGHTS)[0]
    
    # 创建根 span
    root_span_id = generate_id().replace("-", "")[:16]
    start_time = current_time_nanos()
    
    # 模拟请求处理时间（毫秒）
    duration_ms = random.uniform(50, 500)
    end_time = start_time + int(duration_ms * 1_000_000)
    
    # 创建 span 列表
    spans = [{
        "traceId": trace_id,
        "spanId": root_span_id,
        "name": f"{method} {endpoint}",
        "kind": 2,  # SERVER
        "startTimeUnixNano": str(start_time),
        "endTimeUnixNano": str(end_time),
        "attributes": [
            {"key": "http.method", "value": {"stringValue": method}},
            {"key": "http.url", "value": {"stringValue": f"https://example.com{endpoint}"}},
            {"key": "http.status_code", "value": {"intValue": status_code}},
            {"key": "http.client_ip", "value": {"stringValue": client_ip}},
            {"key": "user.id", "value": {"stringValue": user_id}},
            {"key": "http.request_duration_ms", "value": {"doubleValue": duration_ms}}
        ],
        "status": {
            "code": 0 if status_code < 400 else 2,  # OK or ERROR
            "message": "" if status_code < 400 else f"HTTP error {status_code}"
        }
    }]
    
    # 如果是 GET /api/products，添加数据库查询 span
    if method == "GET" and endpoint == "/api/products":
        db_span_id = generate_id().replace("-", "")[:16]
        db_start_time = start_time + int(random.uniform(10, 50) * 1_000_000)
        db_duration_ms = random.uniform(30, 200)
        db_end_time = db_start_time + int(db_duration_ms * 1_000_000)
        
        spans.append({
            "traceId": trace_id,
            "spanId": db_span_id,
            "parentSpanId": root_span_id,
            "name": "database.query",
            "kind": 3,  # CLIENT
            "startTimeUnixNano": str(db_start_time),
            "endTimeUnixNano": str(db_end_time),
            "attributes": [
                {"key": "db.system", "value": {"stringValue": "postgresql"}},
                {"key": "db.name", "value": {"stringValue": "products_db"}},
                {"key": "db.operation", "value": {"stringValue": "SELECT"}}
            ],
            "status": {"code": 0}  # OK
        })
    
    # 如果是 POST /api/orders，添加订单创建和支付处理 spans
    if method == "POST" and endpoint == "/api/orders":
        # 订单创建 span
        order_span_id = generate_id().replace("-", "")[:16]
        order_start_time = start_time + int(random.uniform(10, 30) * 1_000_000)
        order_duration_ms = random.uniform(30, 100)
        order_end_time = order_start_time + int(order_duration_ms * 1_000_000)
        
        order_id = generate_id()
        order_amount = round(random.uniform(10.0, 500.0), 2)
        
        spans.append({
            "traceId": trace_id,
            "spanId": order_span_id,
            "parentSpanId": root_span_id,
            "name": "create_order",
            "kind": 1,  # INTERNAL
            "startTimeUnixNano": str(order_start_time),
            "endTimeUnixNano": str(order_end_time),
            "attributes": [
                {"key": "order.id", "value": {"stringValue": order_id}},
                {"key": "order.amount", "value": {"doubleValue": order_amount}}
            ],
            "status": {"code": 0}  # OK
        })
        
        # 支付处理 span
        payment_span_id = generate_id().replace("-", "")[:16]
        payment_start_time = order_end_time + int(random.uniform(5, 15) * 1_000_000)
        payment_duration_ms = random.uniform(100, 300)
        payment_end_time = payment_start_time + int(payment_duration_ms * 1_000_000)
        
        payment_provider = random.choice(["stripe", "paypal", "credit_card"])
        payment_status = random.choice(["success", "success", "success", "failed"])
        
        spans.append({
            "traceId": trace_id,
            "spanId": payment_span_id,
            "parentSpanId": root_span_id,
            "name": "process_payment",
            "kind": 3,  # CLIENT
            "startTimeUnixNano": str(payment_start_time),
            "endTimeUnixNano": str(payment_end_time),
            "attributes": [
                {"key": "payment.provider", "value": {"stringValue": payment_provider}},
                {"key": "payment.status", "value": {"stringValue": payment_status}}
            ],
            "status": {"code": 0 if payment_status == "success" else 2}
        })
    
    # 创建 OTLP 格式的 trace 数据
    trace_data = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": SERVICE_NAME}},
                        {"key": "service.version", "value": {"stringValue": SERVICE_VERSION}},
                        {"key": "environment", "value": {"stringValue": "production"}}
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "demo.trace"
                        },
                        "spans": spans
                    }
                ]
            }
        ]
    }
    
    return trace_data

# 发送 trace 数据到 OpenSearch Ingestion Pipeline
def send_trace(trace_data):
    auth = create_aws_auth()
    
    response = requests.post(
        f"{INGESTION_URL}/v1/traces",
        json=trace_data,
        auth=auth
    )
    
    if response.status_code != 200:
        print(f"发送失败: {response.status_code} {response.text}")
        return False
    
    return True

def main():
    print(f"开始模拟 HTTP 请求并发送 trace 数据到 {INGESTION_URL}")
    
    try:
        # 模拟一系列请求
        for i in range(50):
            trace_data = create_trace()
            success = send_trace(trace_data)
            
            if success:
                print(f"已发送请求 {i+1}/50")
            else:
                print(f"请求 {i+1}/50 发送失败")
            
            # 随机间隔，模拟真实流量
            time.sleep(random.uniform(0.2, 1.0))
    except Exception as e:
        print(f"发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("模拟完成")

if __name__ == "__main__":
    main()
