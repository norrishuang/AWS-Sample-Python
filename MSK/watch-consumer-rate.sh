#!/bin/bash

# 参数设置
BOOTSTRAP_SERVER="b-1.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092,b-2.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092"  # Kafka 服务器地址
CONSUMER_GROUP="opensearchingestion"  # 要监控的消费组名称
INTERVAL=5  # 刷新间隔（秒）

# 查找 kafka-consumer-groups.sh 脚本
KAFKA_SCRIPT=""
for path in "/home/ec2-user/kafka/bin/kafka-consumer-groups.sh" "/opt/kafka/bin/kafka-consumer-groups.sh" "$(which kafka-consumer-groups.sh 2>/dev/null)"; do
  if [ -x "$path" ]; then
    KAFKA_SCRIPT="$path"
    break
  fi
done

# 检查必要工具
if [ -z "$KAFKA_SCRIPT" ]; then
  echo "错误: 找不到 kafka-consumer-groups.sh 脚本"
  echo "请安装 Kafka 工具或指定正确的路径"
  exit 1
fi

# 创建临时文件存储上一次的 lag 值
TEMP_FILE=$(mktemp)
PREV_FILE=$(mktemp)
echo "临时文件: $TEMP_FILE, $PREV_FILE"

# 清理函数
cleanup() {
  echo "清理临时文件..."
  rm -f "$TEMP_FILE" "$PREV_FILE"
  exit 0
}

# 设置信号处理
trap cleanup SIGINT SIGTERM EXIT

# 直接运行监控循环
echo "开始监控消费组: $CONSUMER_GROUP"
echo "按 Ctrl+C 停止监控"

while true; do
  clear
  echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "消费组: $CONSUMER_GROUP"
  echo "---------------------------------------------------------------------------------------------------------"
  echo "主题                分区    当前偏移量    日志结束偏移量    LAG    消费速率(msg/s)    消费者ID"
  echo "---------------------------------------------------------------------------------------------------------"
  
  # 获取当前状态并保存到临时文件
  "$KAFKA_SCRIPT" --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe --group "$CONSUMER_GROUP" 2>/dev/null > "$TEMP_FILE"
  
  # 检查命令是否成功
  if [ $? -ne 0 ]; then
    echo "错误: 无法获取消费组信息，请检查连接和消费组名称"
    sleep "$INTERVAL"
    continue
  fi
  
  # 跳过标题行
  sed -i '1d' "$TEMP_FILE"
  
  # 如果存在上一次的数据，计算消费速率
  if [ -s "$PREV_FILE" ]; then
    # 处理每一行，计算消费速率
    while IFS= read -r line; do
      # 使用awk提取字段，适应实际输出格式
      group=$(echo "$line" | awk '{print $1}')
      topic=$(echo "$line" | awk '{print $2}')
      partition=$(echo "$line" | awk '{print $3}')
      current_offset=$(echo "$line" | awk '{print $4}')
      log_end=$(echo "$line" | awk '{print $5}')
      lag=$(echo "$line" | awk '{print $6}')
      consumer_id=$(echo "$line" | awk '{print $7}')
      
      # 处理当前偏移量为 "-" 的情况
      if [ "$current_offset" = "-" ]; then
        rate="未分配"
        printf "%-20s %-8s %-14s %-18s %-7s %-18s %-20s\n" "$topic" "$partition" "$current_offset" "$log_end" "$lag" "$rate" "$consumer_id"
        continue
      fi
      
      # 查找上一次的相同主题和分区
      prev_line=$(grep -E "$topic[[:space:]]+$partition[[:space:]]+" "$PREV_FILE")
      if [ -n "$prev_line" ]; then
        prev_offset=$(echo "$prev_line" | awk '{print $4}')
        
        # 如果上一次偏移量也是 "-"，则无法计算速率
        if [ "$prev_offset" = "-" ]; then
          rate="未分配"
        else
          # 计算消费速率（每秒消息数）
          rate=$((current_offset - prev_offset))
          if [ $rate -lt 0 ]; then
            rate=0  # 处理偏移量重置的情况
          fi
        fi
        
        printf "%-20s %-8s %-14s %-18s %-7s %-18s %-20s\n" "$topic" "$partition" "$current_offset" "$log_end" "$lag" "$rate" "$consumer_id"
      else
        printf "%-20s %-8s %-14s %-18s %-7s %-18s %-20s\n" "$topic" "$partition" "$current_offset" "$log_end" "$lag" "计算中..." "$consumer_id"
      fi
    done < "$TEMP_FILE"
  else
    # 第一次运行，无法计算速率
    while IFS= read -r line; do
      group=$(echo "$line" | awk '{print $1}')
      topic=$(echo "$line" | awk '{print $2}')
      partition=$(echo "$line" | awk '{print $3}')
      current_offset=$(echo "$line" | awk '{print $4}')
      log_end=$(echo "$line" | awk '{print $5}')
      lag=$(echo "$line" | awk '{print $6}')
      consumer_id=$(echo "$line" | awk '{print $7}')
      
      rate="计算中..."
      if [ "$current_offset" = "-" ]; then
        rate="未分配"
      fi
      
      printf "%-20s %-8s %-14s %-18s %-7s %-18s %-20s\n" "$topic" "$partition" "$current_offset" "$log_end" "$lag" "$rate" "$consumer_id"
    done < "$TEMP_FILE"
  fi
  
  # 保存当前状态用于下次计算
  cp "$TEMP_FILE" "$PREV_FILE"
  
  # 等待指定的间隔时间
  sleep "$INTERVAL"
done
