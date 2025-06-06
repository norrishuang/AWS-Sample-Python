#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import signal
import sys
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Union
from io import StringIO

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:
    print("错误: 未安装confluent-kafka库")
    print("请使用以下命令安装: pip install confluent-kafka")
    sys.exit(1)


class KafkaLatencyMonitor:
    def __init__(self, args):
        self.bootstrap_servers = args.bootstrap_servers
        self.topic = args.topic
        self.group_id = args.group_id
        self.use_kafka_timestamp = args.use_kafka_timestamp
        self.timestamp_field = args.timestamp_field
        self.from_beginning = args.from_beginning
        self.stats_interval = args.stats_interval
        self.stats_enabled = args.stats_enabled
        self.log_file = args.output
        self.batch_size = args.batch_size
        self.poll_timeout = args.poll_timeout
        
        # 初始化统计变量
        self.total_messages = 0
        self.total_latency = 0
        self.min_latency = float('inf')
        self.max_latency = 0
        self.last_stats_time = time.time()
        self.message_buffer = []
        self.log_buffer = StringIO()
        self.buffer_lock = threading.Lock()
        
        # 性能监控变量
        self.last_message_count = 0
        self.last_performance_check = time.time()
        self.consumption_rate = 0  # 消息/秒
        
        # 短期性能监控变量（用于更准确地计算消费速率）
        self.short_term_start_time = time.time()
        self.short_term_start_count = 0
        self.short_term_message_count = 0
        self.short_term_rate = 0  # 短期消息/秒
        
        # 新增: 延迟统计变量
        self.latency_values = []  # 存储所有延迟值，用于计算中位数
        self.latency_distribution = {
            "0-10ms": 0,
            "10-50ms": 0,
            "50-100ms": 0,
            "100-500ms": 0,
            "500-1000ms": 0,
            "1s+": 0
        }
        # 记录异常延迟的消息数量
        self.high_latency_count = 0
        
        # 初始化日志文件
        self.init_log_file()
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
        
        # 初始化Kafka消费者
        self.init_consumer()
        
        # 启动日志写入线程
        self.running = True
        self.log_thread = threading.Thread(target=self.log_writer_thread)
        self.log_thread.daemon = True
        self.log_thread.start()
        
    def init_log_file(self):
        """初始化日志文件"""
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write(f"# Kafka消费延迟监控日志\n")
            f.write(f"# 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 主题: {self.topic}\n")
            f.write(f"# 服务器: {self.bootstrap_servers}\n")
            if self.use_kafka_timestamp:
                f.write("# 使用Kafka内置时间戳\n")
            else:
                f.write(f"# 使用消息中的时间戳字段: {self.timestamp_field}\n")
            if self.from_beginning:
                f.write("# 从头开始消费消息\n")
            else:
                f.write("# 从最新消息开始消费\n")
            f.write(f"# 统计信息间隔: {self.stats_interval}秒\n")
            f.write(f"# 批处理大小: {self.batch_size}条消息\n")
            f.write(f"# 轮询超时: {self.poll_timeout}秒\n")
            f.write("----------------------------------------\n")
    
    def buffer_log(self, message):
        """将日志消息添加到缓冲区"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with self.buffer_lock:
            self.log_buffer.write(f"[{timestamp}] {message}\n")
    
    def log_writer_thread(self):
        """日志写入线程，定期将缓冲区内容写入文件"""
        while self.running:
            time.sleep(1)  # 每秒检查一次
            with self.buffer_lock:
                buffer_content = self.log_buffer.getvalue()
                if buffer_content:
                    self.log_buffer = StringIO()  # 重置缓冲区
            
            if buffer_content:
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(buffer_content)
    
    def init_consumer(self):
        """初始化Kafka消费者"""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest' if self.from_beginning else 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,  # 增加提交间隔到5秒
            'fetch.min.bytes': 1,  # 增加最小拉取字节数
            'fetch.error.backoff.ms': 500,  # 错误后的退避时间
            'max.partition.fetch.bytes': 1048576,  # 每个分区的最大拉取字节数
            'fetch.max.bytes': 52428800,  # 50MB，增加最大拉取字节数
            'session.timeout.ms': 30000,  # 会话超时时间
            'heartbeat.interval.ms': 10000,  # 心跳间隔
            'socket.receive.buffer.bytes': 1048576,  # 增加接收缓冲区大小
            'socket.send.buffer.bytes': 1048576  # 增加发送缓冲区大小
        }
        
        self.consumer = Consumer(config)
        self.consumer.subscribe([self.topic])
        
        self.buffer_log(f"Kafka消费者已初始化")
        self.buffer_log(f"消费者组ID: {self.group_id}")
        self.buffer_log(f"自动偏移量重置: {'earliest' if self.from_beginning else 'latest'}")
        self.buffer_log("----------------------------------------")
    
    def format_time(self, timestamp_ms):
        """格式化时间戳"""
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        return dt.strftime('%Y-%m-%d %H:%M:%S.') + f"{timestamp_ms % 1000:03d}"
    
    def print_stats(self):
        """输出统计信息"""
        if self.total_messages == 0:
            self.buffer_log("统计信息: 尚未收到任何消息")
            return
        
        avg_latency = self.total_latency / self.total_messages
        
        # 计算中位数延迟
        median_latency = sorted(self.latency_values)[len(self.latency_values) // 2]
        
        # 计算消费速率 - 长期平均
        current_time = time.time()
        time_diff = current_time - self.last_performance_check
        if time_diff > 0:
            message_diff = self.total_messages - self.last_message_count
            self.consumption_rate = message_diff / time_diff
            self.last_message_count = self.total_messages
            self.last_performance_check = current_time
        
        # 使用短期速率（如果可用）
        consumption_rate_display = self.short_term_rate if self.short_term_rate > 0 else self.consumption_rate
        
        self.buffer_log("")
        self.buffer_log("========== 统计信息 ==========")
        self.buffer_log(f"消息总数: {self.total_messages}")
        self.buffer_log(f"平均延迟: {avg_latency:.2f}ms ({avg_latency/1000:.2f}秒)")
        self.buffer_log(f"中位数延迟: {median_latency:.2f}ms ({median_latency/1000:.2f}秒)")
        self.buffer_log(f"最小延迟: {self.min_latency:.2f}ms ({self.min_latency/1000:.2f}秒)")
        self.buffer_log(f"最大延迟: {self.max_latency:.2f}ms ({self.max_latency/1000:.2f}秒)")
        self.buffer_log(f"消费速率: {consumption_rate_display:.2f}消息/秒")
        if self.short_term_rate > 0:
            self.buffer_log(f"短期消费速率(10秒): {self.short_term_rate:.2f}消息/秒")
            self.buffer_log(f"长期消费速率: {self.consumption_rate:.2f}消息/秒")
        
        # 输出延迟分布
        self.buffer_log("\n延迟分布:")
        for range_name, count in self.latency_distribution.items():
            percentage = (count / self.total_messages) * 100 if self.total_messages > 0 else 0
            self.buffer_log(f"  {range_name}: {count} 消息 ({percentage:.2f}%)")
        
        # 输出异常延迟信息
        if self.high_latency_count > 0:
            high_latency_percentage = (self.high_latency_count / self.total_messages) * 100
            self.buffer_log(f"\n检测到 {self.high_latency_count} 条异常延迟消息 ({high_latency_percentage:.2f}%)")
        
        self.buffer_log("==============================")
        self.buffer_log("")
        
        print(f"统计信息 - 消息总数: {self.total_messages}, 平均延迟: {avg_latency:.2f}ms, 中位数延迟: {median_latency:.2f}ms, 消费速率: {consumption_rate_display:.2f}消息/秒")
    
    def handle_signal(self, signum, frame):
        """处理信号"""
        self.buffer_log("")
        self.buffer_log("监控已停止")
        self.print_stats()
        self.buffer_log("")
        
        # 停止日志线程
        self.running = False
        self.log_thread.join(timeout=2)
        
        # 确保最后的日志被写入
        with self.buffer_lock:
            buffer_content = self.log_buffer.getvalue()
        
        if buffer_content:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(buffer_content)
        
        print(f"监控已停止，结果已保存到 {self.log_file}")
        sys.exit(0)
    
    def extract_timestamp_from_message(self, message):
        """从消息中提取时间戳"""
        # 首先尝试从日志行开头提取时间戳
        try:
            # 日志格式示例: 2025-03-27 07:06:50.128 [http-nio-8074-exec-736] ...
            timestamp_str = message.split(' ', 2)[0] + ' ' + message.split(' ', 2)[1]
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
            # 转换为毫秒时间戳
            return int(dt.timestamp() * 1000)
        except (ValueError, IndexError):
            # 如果无法从日志开头提取时间戳，尝试解析JSON
            try:
                # 尝试解析JSON
                data = json.loads(message)
                if self.timestamp_field in data:
                    return int(data[self.timestamp_field])
                else:
                    self.buffer_log(f"警告: 消息中不存在时间戳字段 '{self.timestamp_field}'")
                    return None
            except json.JSONDecodeError:
                # 如果不是JSON格式，尝试正则表达式匹配时间戳
                import re
                timestamp_pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?:\.\d+)?)'
                match = re.search(timestamp_pattern, message)
                if match:
                    try:
                        timestamp_str = match.group(1)
                        if '.' in timestamp_str:
                            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                        else:
                            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        return int(dt.timestamp() * 1000)
                    except ValueError:
                        self.buffer_log(f"警告: 无法解析时间戳格式: {timestamp_str}")
                        return None
                else:
                    self.buffer_log(f"警告: 消息中未找到时间戳")
                    return None
            except (ValueError, TypeError):
                self.buffer_log(f"警告: 时间戳字段 '{self.timestamp_field}' 不是有效的数字")
                return None
    
    def process_message_batch(self, messages):
        """批量处理消息"""
        if not messages:
            return
        
        batch_summary = []
        total_batch_latency = 0
        batch_latency_values = []  # 记录本批次的所有延迟值
        
        # 更新短期消费速率计算
        current_time = time.time()
        self.short_term_message_count += len(messages)
        
        # 每10秒重置一次短期统计
        if current_time - self.short_term_start_time >= 10:
            time_diff = current_time - self.short_term_start_time
            message_diff = self.short_term_message_count - self.short_term_start_count
            if time_diff > 0:
                self.short_term_rate = message_diff / time_diff
            self.short_term_start_time = current_time
            self.short_term_start_count = self.short_term_message_count
        
        for msg_data in messages:
            msg, current_time, producer_time, latency = msg_data
            
            # 更新统计信息
            self.total_messages += 1
            self.total_latency += latency
            self.min_latency = min(self.min_latency, latency)
            self.max_latency = max(self.max_latency, latency)
            self.latency_values.append(latency)  # 添加到总延迟值列表
            batch_latency_values.append(latency)  # 添加到批次延迟值列表
            
            # 更新延迟分布
            if latency < 10:
                self.latency_distribution["0-10ms"] += 1
            elif latency < 50:
                self.latency_distribution["10-50ms"] += 1
            elif latency < 100:
                self.latency_distribution["50-100ms"] += 1
            elif latency < 500:
                self.latency_distribution["100-500ms"] += 1
            elif latency < 1000:
                self.latency_distribution["500-1000ms"] += 1
            else:
                self.latency_distribution["1s+"] += 1
            
            # 记录异常高的延迟
            if latency > 100:  # 超过100ms视为异常
                self.high_latency_count += 1
                self.buffer_log(f"检测到异常延迟: {latency:.2f}ms, 生产时间: {self.format_time(producer_time)}, 消费时间: {self.format_time(current_time)}")
            
            # 添加到批次摘要
            batch_summary.append({
                'producer_time': self.format_time(producer_time),
                'consumer_time': self.format_time(current_time),
                'latency_ms': latency,
                'latency_sec': latency/1000
            })
            
            total_batch_latency += latency
        
        # 记录批次摘要
        avg_batch_latency = total_batch_latency / len(messages)
        
        # 计算批次中位数延迟
        batch_median_latency = sorted(batch_latency_values)[len(batch_latency_values) // 2]
        
        self.buffer_log(f"处理批次: {len(messages)}条消息")
        self.buffer_log(f"批次平均延迟: {avg_batch_latency:.2f}ms ({avg_batch_latency/1000:.2f}秒)")
        self.buffer_log(f"批次中位数延迟: {batch_median_latency:.2f}ms ({batch_median_latency/1000:.2f}秒)")
        
        # 只记录前3条和后3条消息的详细信息
        if len(messages) <= 6:
            for i, summary in enumerate(batch_summary):
                self.buffer_log(f"消息 {i+1}: 生产时间={summary['producer_time']}, 消费时间={summary['consumer_time']}, 延迟={summary['latency_ms']:.2f}ms")
        else:
            for i in range(3):
                self.buffer_log(f"消息 {i+1}: 生产时间={batch_summary[i]['producer_time']}, 消费时间={batch_summary[i]['consumer_time']}, 延迟={batch_summary[i]['latency_ms']:.2f}ms")
            self.buffer_log("...")
            for i in range(3):
                idx = len(batch_summary) - 3 + i
                self.buffer_log(f"消息 {len(messages)-2+i}: 生产时间={batch_summary[idx]['producer_time']}, 消费时间={batch_summary[idx]['consumer_time']}, 延迟={batch_summary[idx]['latency_ms']:.2f}ms")
        
        self.buffer_log("----------------------------------------")
        
        # 打印进度
        print(f"已处理 {self.total_messages} 条消息，当前批次平均延迟: {avg_batch_latency:.2f}ms, 中位数延迟: {batch_median_latency:.2f}ms")
        
        # 检测异常延迟
        if avg_batch_latency > 60000:  # 如果平均延迟超过1分钟
            self.buffer_log(f"警告: 检测到异常高延迟! 平均延迟: {avg_batch_latency:.2f}ms ({avg_batch_latency/1000:.2f}秒)")
            print(f"警告: 检测到异常高延迟! 平均延迟: {avg_batch_latency:.2f}ms ({avg_batch_latency/1000:.2f}秒)")
            
            # 如果延迟异常高，可能是时间戳解析问题，记录一些消息样本以便调试
            if len(messages) > 0:
                try:
                    sample_msg = messages[0][0].value().decode('utf-8', errors='replace')
                    self.buffer_log(f"消息样本: {sample_msg[:200]}...")
                    print(f"消息样本: {sample_msg[:200]}...")
                except Exception as e:
                    self.buffer_log(f"无法解码消息样本: {str(e)}")
                    print(f"无法解码消息样本: {str(e)}")
    
    def run(self):
        """运行监控"""
        print(f"开始持续监控Kafka消费延迟...")
        print(f"监控结果将写入文件: {self.log_file}")
        print(f"按Ctrl+C停止监控")
        
        message_batch = []
        last_batch_time = time.time()
        batch_start_time = time.time()
        
        try:
            while True:
                msg = self.consumer.poll(self.poll_timeout)
                
                if msg is None:
                    # 如果没有新消息但有积累的消息，处理它们
                    if message_batch:
                        self.process_message_batch(message_batch)
                        message_batch = []
                        batch_start_time = time.time()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # 到达分区末尾
                        continue
                    else:
                        # 其他错误
                        self.buffer_log(f"错误: {msg.error()}")
                        continue
                
                # 获取当前时间戳（毫秒）
                current_time = time.time_ns() // 1000000  # 纳秒转毫秒
                
                # 获取生产时间戳
                if self.use_kafka_timestamp:
                    # 使用Kafka内置时间戳
                    producer_time = msg.timestamp()[1]  # (timestamp_type, timestamp)
                    if producer_time <= 0:  # 如果Kafka时间戳无效
                        message = msg.value().decode('utf-8', errors='replace')
                        producer_time = self.extract_timestamp_from_message(message)
                        if producer_time is None:
                            continue
                else:
                    # 从消息内容中提取时间戳
                    try:
                        message = msg.value().decode('utf-8', errors='replace')
                        producer_time = self.extract_timestamp_from_message(message)
                        if producer_time is None:
                            continue
                    except UnicodeDecodeError:
                        self.buffer_log(f"警告: 无法解码消息内容")
                        continue
                
                # 计算延迟（毫秒）
                latency = current_time - producer_time
                
                # 添加到批处理队列
                message_batch.append((msg, current_time, producer_time, latency))
                
                # 如果达到批处理大小或者距离上次处理已经超过1秒，处理批次
                current_time_sec = time.time()
                if len(message_batch) >= self.batch_size or (current_time_sec - last_batch_time >= 1 and len(message_batch) > 0):
                    self.process_message_batch(message_batch)
                    message_batch = []
                    last_batch_time = current_time_sec
                    batch_start_time = current_time_sec
                
                # 定期输出统计信息
                if self.stats_enabled:
                    current_stats_time = time.time()
                    if current_stats_time - self.last_stats_time >= self.stats_interval:
                        self.print_stats()
                        self.last_stats_time = current_stats_time
                
        except KeyboardInterrupt:
            pass
        finally:
            # 处理剩余的消息
            if message_batch:
                self.process_message_batch(message_batch)
            
            self.running = False
            if self.log_thread.is_alive():
                self.log_thread.join(timeout=2)
            self.consumer.close()


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Kafka消费延迟监控工具')
    parser.add_argument('-b', '--bootstrap-servers', default='localhost:9092',
                        help='Kafka服务器地址 (默认: localhost:9092)')
    parser.add_argument('-t', '--topic', required=True,
                        help='要消费的主题')
    parser.add_argument('-g', '--group-id', default='latency-monitor-group',
                        help='消费者组ID (默认: latency-monitor-group)')
    parser.add_argument('-k', '--use-kafka-timestamp', action='store_true',
                        help='使用Kafka内置时间戳 (默认: False)')
    parser.add_argument('-f', '--timestamp-field', default='timestamp',
                        help='消息中时间戳字段名，仅当不使用Kafka时间戳时有效 (默认: timestamp)')
    parser.add_argument('--from-beginning', action='store_true',
                        help='是否从头开始消费 (默认: False，从最新消息开始)')
    parser.add_argument('-i', '--stats-interval', type=int, default=10,
                        help='统计信息输出间隔（秒）(默认: 10)')
    parser.add_argument('--no-stats', dest='stats_enabled', action='store_false',
                        help='禁用统计信息输出')
    parser.add_argument('-o', '--output', 
                        default=f"kafka_latency_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                        help='日志输出文件')
    parser.add_argument('-bs', '--batch-size', type=int, default=100,
                        help='消息批处理大小 (默认: 100)')
    parser.add_argument('-pt', '--poll-timeout', type=float, default=0.5,
                        help='消费者轮询超时时间（秒）(默认: 0.5)')
    parser.set_defaults(stats_enabled=True, use_kafka_timestamp=False)
    
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    monitor = KafkaLatencyMonitor(args)
    monitor.run()
