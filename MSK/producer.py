#!/usr/bin/env python3
import os
import time
import json
from kafka import KafkaProducer
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 从环境变量获取配置
BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.environ.get('KAFKA_TOPIC', 'lazada-logs')
DATA_FILE = os.environ.get('DATA_FILE', '/app/data/lazada-log-002.txt')
INTERVAL_SECONDS = float(os.environ.get('INTERVAL_SECONDS', '0.1'))  # 每条消息之间的间隔时间

def create_producer():
    """创建 Kafka Producer 实例"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # 如果需要安全连接，取消下面的注释并提供正确的配置
            # security_protocol="SSL",
            # ssl_cafile="/path/to/ca.pem",
            # ssl_certfile="/path/to/cert.pem",
            # ssl_keyfile="/path/to/key.pem",
        )
        return producer
    except Exception as e:
        logger.error(f"创建 Kafka Producer 失败: {e}")
        raise

def read_log_file(file_path):
    """读取日志文件内容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.readlines()
    except Exception as e:
        logger.error(f"读取文件 {file_path} 失败: {e}")
        raise

def extract_log_data(line):
    """从日志行提取有用的数据"""
    # 这里可以根据实际日志格式进行解析
    # 简单示例：将整行作为消息内容
    return {
        "timestamp": time.time(),
        "log_content": line.strip(),
        "source": "lazada-sample"
    }

def main():
    """主函数：循环读取日志并发送到 Kafka"""
    logger.info(f"开始向 Kafka 发送数据，Bootstrap Servers: {BOOTSTRAP_SERVERS}, Topic: {TOPIC_NAME}")
    
    try:
        # 创建 Kafka Producer
        producer = create_producer()
        
        # 读取日志文件
        logger.info(f"读取数据文件: {DATA_FILE}")
        log_lines = read_log_file(DATA_FILE)
        logger.info(f"共读取 {len(log_lines)} 行数据")
        
        # 无限循环发送数据
        count = 0
        while True:
            for line in log_lines:
                if not line.strip():
                    continue
                    
                # 提取日志数据
                log_data = extract_log_data(line)
                
                # 发送到 Kafka
                future = producer.send(TOPIC_NAME, value=log_data)
                # 可选：等待发送结果
                # record_metadata = future.get(timeout=10)
                
                count += 1
                if count % 100 == 0:
                    logger.info(f"已发送 {count} 条消息")
                
                # 控制发送速率
                time.sleep(INTERVAL_SECONDS)
                
    except KeyboardInterrupt:
        logger.info("程序被手动中断")
    except Exception as e:
        logger.error(f"发生错误: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()
            producer.close()
            logger.info("Kafka Producer 已关闭")

if __name__ == "__main__":
    main()
