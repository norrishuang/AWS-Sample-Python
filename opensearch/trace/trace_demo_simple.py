#!/usr/bin/env python3
"""
OpenSearch Trace Demo - 简化版本，直接使用 HTTP 请求发送 trace 数据
基于 OpenSearch Ingestion Pipeline 的配置示例
"""

import time
import random
import uuid
import json
import requests
import boto3
import datetime
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

# OpenSearch Ingestion Pipeline URL
INGESTION_URL = "https://trace-logs-demo-wwxfoyslntxmqkwjzgnftufjq4.us-east-1.osis.amazonaws.com"

# AWS 区域
AWS_REGION = "us-east-1"  # 根据你的 OpenSearch 服务所在区域调整

# 模拟的服务名称
SERVICE_NAME = "demo-web-service"

# 使用 botocore 的 SigV4Auth 进行请求签名
def sign_request(url, body):
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    
    print(f"使用 AWS 凭证: {credentials.access_key[:4]}...{credentials.access_key[-4:]}")
    print(f"AWS 区域: {AWS_REGION}")
    print(f"AWS 服务: osis")
    print(f"目标 URL: {url}")
    
    # 创建 AWS 请求对象
    request = AWSRequest(
        method='POST',
        url=url,
        data=json.dumps(body)
    )
    
    # 添加必要的头信息
    request.headers['Content-Type'] = 'application/json'
    request.headers['Host'] = url.replace('https://', '').split('/')[0]
    
    # 使用 SigV4 签名请求
    auth = SigV4Auth(credentials, 'osis', AWS_REGION)
    auth.add_auth(request)
    
    return request.headers

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
    url = f"{INGESTION_URL}/v1/traces"
    
    try:
        # 获取签名后的头信息
        headers = sign_request(url, trace_data)
        
        # 打印请求信息
        print(f"\n发送请求到: {url}")
        print(f"请求头:")
        for key, value in headers.items():
            if key.lower() not in ('authorization'):  # 不打印敏感信息
                print(f"  {key}: {value}")
        
        # 发送请求
        response = requests.post(
            url,
            json=trace_data,
            headers=headers
        )
        
        print(f"响应状态码: {response.status_code}")
        if response.status_code != 200:
            print(f"响应内容: {response.text}")
        
        return response.status_code == 200
    except Exception as e:
        print(f"发送请求时出错: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print(f"开始发送简单的 trace 数据到 {INGESTION_URL}")
    
    # 发送 5 个简单的 trace
    for i in range(5):
        print(f"\n生成并发送 trace {i+1}/5...")
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
