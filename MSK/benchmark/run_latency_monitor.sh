#!/bin/bash

# 设置默认参数
TOPIC=${1:-"lazada-logs"}
BOOTSTRAP_SERVERS=${2:-"localhost:9092"}
GROUP_ID="latency-monitor-group-$(date +%s)"

echo "启动Kafka延迟监控..."
echo "主题: $TOPIC"
echo "服务器: $BOOTSTRAP_SERVERS"
echo "消费者组ID: $GROUP_ID"

# 运行监控脚本
python kafka_latency_monitor_improved.py \
  -t "$TOPIC" \
  -b "$BOOTSTRAP_SERVERS" \
  -g "$GROUP_ID" \
  -bs 100 \
  -pt 1.0 \
  -i 5 \
  -k
