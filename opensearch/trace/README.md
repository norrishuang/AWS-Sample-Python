# OpenSearch Trace Demo

这个演示项目展示了如何生成模拟的 trace 数据并将其发送到 Amazon OpenSearch Ingestion Pipeline。

## 项目结构

- `trace_demo.py` - 使用 OpenTelemetry 库生成和发送 trace 数据
- `trace_demo_alternative.py` - 使用直接 HTTP 请求生成和发送 trace 数据（备选方案）
- `requirements.txt` - 项目依赖

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行演示

使用 OpenTelemetry 方式（推荐）:
```bash
python trace_demo.py
```

或者使用直接 HTTP 请求方式:
```bash
python trace_demo_alternative.py
```

## AWS 认证

这个演示使用 AWS SigV4 认证来访问 OpenSearch Ingestion Pipeline。请确保你的环境已正确配置 AWS 凭证。你可以通过以下方式配置：

1. 使用 AWS CLI 配置凭证：
```bash
aws configure
```

2. 或者设置环境变量：
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_SESSION_TOKEN=your_session_token  # 如果使用临时凭证
```

## 功能说明

这个演示程序会：

1. 初始化 OpenTelemetry 追踪系统（或直接创建 OTLP 格式的 trace 数据）
2. 模拟多个 HTTP 请求，包括不同的：
   - API 端点
   - HTTP 方法
   - 用户 ID
   - 客户端 IP
   - 响应状态码
3. 为每个请求创建完整的 trace，包括：
   - 主请求 span
   - 数据库查询 span（对于某些请求）
   - 订单创建和支付处理 span（对于 POST /api/orders）
4. 将生成的 trace 数据发送到配置的 OpenSearch Ingestion Pipeline

## 自定义配置

你可以在脚本中修改以下变量来自定义演示：

- `INGESTION_URL` - OpenSearch Ingestion Pipeline 的 URL
- `AWS_REGION` - AWS 区域
- `SERVICE_NAME` - 模拟的服务名称
- `API_ENDPOINTS` - 模拟的 API 端点列表
- `HTTP_METHODS` - 使用的 HTTP 方法
- `HTTP_STATUS_CODES` 和 `HTTP_STATUS_WEIGHTS` - 状态码及其出现概率

## 查看结果

发送数据后，你可以在 OpenSearch Dashboard 中查看生成的 trace 数据。通常可以在 Trace Analytics 部分找到这些数据。
