FROM python:3.9-slim AS base

# 安装Filebeat
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | gpg --dearmor -o /usr/share/keyrings/elastic.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/elastic.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | tee /etc/apt/sources.list.d/elastic-8.x.list && \
    apt-get update && \
    apt-get install -y filebeat && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用文件
COPY log_generator.py .
COPY lazada-log-002.txt .
COPY start.sh .

# 复制Filebeat配置并设置正确的权限
COPY filebeat.yml /etc/filebeat/filebeat.yml
RUN chmod go-w /etc/filebeat/filebeat.yml && \
    chown root:root /etc/filebeat/filebeat.yml

# 确保脚本可执行
RUN chmod +x start.sh

# 创建日志目录
RUN mkdir -p /app/logs

# 暴露Filebeat端口（如果需要）
EXPOSE 5066

# 启动应用
CMD ["/app/start.sh"]
