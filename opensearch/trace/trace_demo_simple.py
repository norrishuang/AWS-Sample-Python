#!/usr/bin/env python3
"""
OpenSearch Trace Demo - 简化版本，直接使用 HTTP 请求发送 trace 数据
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
INGESTION_URL = "https://trace-logs-demo-wwxfoyslntxmqkwjzgnftufjq4.us-east-1.osis.amazonaws.com"

# AWS 区域
AWS_REGION = "us-east-1"  # 根据你的 OpenSearch 服务所在区域调整

# 模拟的服务名称
SERVICE_NAME = "demo-web-service"

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

# 生成一个简单的 trace 数据
def generate_trace_data():
    # 创建一个简单的 trace
    trace_id = uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    
    # 随机选择一个 API 端点和方法
    endpoints = ["/api/users", "/api/products", "/api/orders"]
    methods = ["GET", "POST", "PUT"]
    
    endpoint = random.choice(endpoints)
    method = random.choice(methods)
    
    # 创建 span 数据
    current_time = int(time.time() * 1_000_000_000)  # 当前时间（纳秒）
    duration = random.randint(50, 500) * 1_000_000  # 持续时间（纳秒）
    
    # 构建 OTLP 格式的 trace 数据
    trace_data = {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": SERVICE_NAME}}
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "demo.trace"
                        },
                        "spans": [
                            {
                                "traceId": trace_id,
                                "spanId": span_id,
                                "name": f"{method} {endpoint}",
                                "kind": 2,  # SERVER
                                "startTimeUnixNano": str(current_time),
                                "endTimeUnixNano": str(current_time + duration),
                                "attributes": [
                                    {"key": "http.method", "value": {"stringValue": method}},
                                    {"key": "http.url", "value": {"stringValue": f"https://example.com{endpoint}"}},
                                    {"key": "http.status_code", "value": {"intValue": 200}}
                                ],
                                "status": {"code": 0}  # OK
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    return trace_data

# 发送 trace 数据到 OpenSearch Ingestion Pipeline
def send_trace_data(trace_data):
    auth = create_aws_auth()
    
    try:
        response = requests.post(
            f"{INGESTION_URL}/v1/traces",
            json=trace_data,
            auth=auth
        )
        
        print(f"响应状态码: {response.status_code}")
        if response.status_code != 200:
            print(f"响应内容: {response.text}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"发送请求时出错: {e}")
        return False

def main():
    print(f"开始发送简单的 trace 数据到 {INGESTION_URL}")
    
    # 发送 10 个简单的 trace
    for i in range(10):
        print(f"生成并发送 trace {i+1}/10...")
        trace_data = generate_trace_data()
        success = send_trace_data(trace_data)
        
        if success:
            print(f"Trace {i+1} 发送成功")
        else:
            print(f"Trace {i+1} 发送失败")
        
        # 等待一小段时间
        time.sleep(1)
    
    print("所有 trace 数据发送完成")

if __name__ == "__main__":
    main()
