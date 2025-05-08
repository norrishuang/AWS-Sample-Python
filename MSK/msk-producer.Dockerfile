FROM python:3.9-slim

WORKDIR /app

# 安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 创建数据目录
RUN mkdir -p /app/data

# 复制应用代码和数据
COPY producer.py /app/
COPY lazada-log-002.txt /app/data/

# 设置环境变量默认值
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV KAFKA_TOPIC=lazada-logs
ENV DATA_FILE=/app/data/lazada-log-002.txt
ENV INTERVAL_SECONDS=0.1

# 运行应用
CMD ["python", "producer.py"]
