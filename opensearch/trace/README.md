# OpenSearch Trace Demo

这个演示项目展示了如何生成模拟的 trace 数据并将其发送到 Amazon OpenSearch Ingestion Pipeline。

## 项目结构

- `trace_demo.py` - 主要的 Python 脚本，用于生成和发送 trace 数据
- `requirements.txt` - 项目依赖

## 安装依赖

```bash
pip install -r requirements.txt
```

## 运行演示

```bash
python trace_demo.py
```

## 功能说明

这个演示程序会：

1. 初始化 OpenTelemetry 追踪系统
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
- `SERVICE_NAME` - 模拟的服务名称
- `API_ENDPOINTS` - 模拟的 API 端点列表
- `HTTP_METHODS` - 使用的 HTTP 方法
- `HTTP_STATUS_CODES` 和 `HTTP_STATUS_WEIGHTS` - 状态码及其出现概率

## 查看结果

发送数据后，你可以在 OpenSearch Dashboard 中查看生成的 trace 数据。通常可以在 Trace Analytics 部分找到这些数据。
