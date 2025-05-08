#!/bin/bash

# 使用环境变量或默认值
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-"localhost:9092"}
LOG_INTERVAL=${LOG_INTERVAL:-0.1}
MAX_LOG_SIZE=${MAX_LOG_SIZE:-100}
BACKUP_COUNT=${BACKUP_COUNT:-10}

# 创建临时配置文件
cp /etc/filebeat/filebeat.yml /tmp/filebeat.yml

# 替换临时配置文件中的变量
# 注意：这里我们需要处理可能包含多个服务器地址的情况
# 将逗号分隔的地址转换为Filebeat配置中的数组格式
BOOTSTRAP_SERVERS_ARRAY=$(echo $KAFKA_BOOTSTRAP_SERVERS | sed 's/,/", "/g')
sed -i "s/\${KAFKA_BOOTSTRAP_SERVERS}/\"$BOOTSTRAP_SERVERS_ARRAY\"/g" /tmp/filebeat.yml

# 以root权限复制回原位置并设置正确权限
cp /tmp/filebeat.yml /etc/filebeat/filebeat.yml
chmod go-w /etc/filebeat/filebeat.yml
chown root:root /etc/filebeat/filebeat.yml

# 启动日志生成器
echo "启动日志生成器..."
python3 log_generator.py --sample /app/lazada-log-002.txt --output /app/server.log --interval $LOG_INTERVAL --max-size $MAX_LOG_SIZE --backup-count $BACKUP_COUNT &
LOG_GEN_PID=$!

# 等待日志文件创建
echo "等待日志文件创建..."
while [ ! -f /app/server.log ]; do
  sleep 1
done

# 启动Filebeat
echo "启动Filebeat..."
filebeat -e -c /etc/filebeat/filebeat.yml &
FILEBEAT_PID=$!

# 捕获SIGINT和SIGTERM信号
trap "echo '正在停止服务...'; kill $LOG_GEN_PID $FILEBEAT_PID; exit 0" SIGINT SIGTERM

# 等待子进程
echo "服务已启动"
wait
