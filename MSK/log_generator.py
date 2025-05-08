#!/usr/bin/env python3
"""
日志生成器 - 持续读取样本日志并写入输出文件，支持日志滚动
"""
import os
import time
import glob
import argparse
import logging
import socket
from datetime import datetime
from logging.handlers import RotatingFileHandler

def setup_logger(log_file, max_bytes=100*1024*1024, backup_count=10):
    """
    设置带有滚动功能的日志记录器
    
    Args:
        log_file: 日志文件路径
        max_bytes: 每个日志文件的最大大小（字节）
        backup_count: 保留的日志文件数量
    
    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger("log_generator")
    logger.setLevel(logging.INFO)
    
    # 创建一个滚动文件处理器
    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    
    # 设置一个简单的格式器（不添加额外的日志格式，只输出消息本身）
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    return logger

def generate_logs(sample_file, output_file, interval_seconds=0.1, max_bytes=100*1024*1024, backup_count=10):
    """
    从样本文件读取日志并持续写入输出文件，支持日志滚动
    
    Args:
        sample_file: 样本日志文件路径
        output_file: 输出日志文件路径
        interval_seconds: 写入间隔时间(秒)
        max_bytes: 每个日志文件的最大大小（字节）
        backup_count: 保留的日志文件数量
    """
    # 读取样本日志
    with open(sample_file, 'r', encoding='utf-8') as f:
        sample_logs = f.readlines()
    
    # 获取主机名或容器ID，用于区分不同实例
    hostname = os.environ.get('POD_NAME', socket.gethostname())
    
    print(f"实例 {hostname} 已加载 {len(sample_logs)} 行样本日志")
    
    # 设置滚动日志记录器
    logger = setup_logger(output_file, max_bytes, backup_count)
    
    # 持续写入日志
    log_index = 0
    log_count = 0
    
    try:
        while True:
            # 获取当前日志行并添加时间戳和实例标识
            log_line = sample_logs[log_index].strip()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            
            # 如果原始日志行已经有日期格式开头，则保留原格式，否则添加时间戳
            if not log_line.startswith(timestamp[:10]):  # 检查是否以日期开头
                timestamped_log = f"{timestamp} [{hostname}] {log_line}"
            else:
                timestamped_log = f"{log_line} [{hostname}]"
            
            # 写入日志
            logger.info(timestamped_log)
            
            # 循环读取样本日志
            log_index = (log_index + 1) % len(sample_logs)
            log_count += 1
            
            # 每100行输出一次状态
            if log_count % 100 == 0:
                print(f"实例 {hostname} 已写入 {log_count} 行日志到 {output_file}")
            
            # 等待指定时间间隔
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        print("程序被用户中断")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='持续生成日志文件，支持日志滚动')
    parser.add_argument('--sample', default='lazada-log-002.txt', help='样本日志文件路径')
    parser.add_argument('--output', default='./server.log', help='输出日志文件路径')
    parser.add_argument('--interval', type=float, default=0.1, help='日志写入间隔(秒)')
    parser.add_argument('--max-size', type=int, default=100, help='每个日志文件的最大大小(MB)')
    parser.add_argument('--backup-count', type=int, default=10, help='保留的日志文件数量')
    
    args = parser.parse_args()
    
    print(f"开始生成日志: 从 {args.sample} 到 {args.output}")
    print(f"日志滚动设置: 每个文件最大 {args.max_size}MB, 保留 {args.backup_count} 个文件")
    
    generate_logs(
        args.sample, 
        args.output, 
        args.interval,
        args.max_size * 1024 * 1024,  # 转换为字节
        args.backup_count
    )
