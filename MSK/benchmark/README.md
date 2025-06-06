# MSK 消费延迟监控工具

这个工具用于监控 Amazon MSK (Managed Streaming for Kafka) 或其他 Kafka 集群的消息消费延迟。它可以帮助您分析消息从生产到消费的时间差，识别潜在的性能问题和瓶颈。

## 功能特点

- 实时监控 Kafka 消息的消费延迟
- 支持使用 Kafka 内置时间戳或消息内容中的时间戳字段
- 提供详细的延迟统计信息，包括平均延迟、中位数延迟和延迟分布
- 检测并报告异常高延迟的消息
- 计算消息消费速率（短期和长期）
- 批量处理消息以提高性能
- 将监控结果保存到日志文件

## 文件说明

- `kafka_latency_monitor_improved.py`: 主要的监控脚本，包含完整的监控逻辑和功能
- `run_latency_monitor.sh`: 便捷的启动脚本，提供简单的命令行接口

## 依赖项

- Python 3.6+
- confluent-kafka 库

安装依赖：
```bash
pip install confluent-kafka
```

## 使用方法

### 使用启动脚本

最简单的方式是使用提供的启动脚本：

```bash
./run_latency_monitor.sh [主题名称] [Kafka服务器地址]
```

例如：
```bash
./run_latency_monitor.sh my-topic kafka-broker:9092
```

默认值：
- 主题: lazada-logs
- 服务器: localhost:9092
- 消费者组ID: 自动生成的唯一ID

### 直接使用 Python 脚本

如果需要更多自定义选项，可以直接使用 Python 脚本：

```bash
python kafka_latency_monitor_improved.py -t <主题> -b <服务器地址> [其他选项]
```

#### 可用选项

- `-b, --bootstrap-servers`: Kafka 服务器地址 (默认: localhost:9092)
- `-t, --topic`: 要消费的主题 (必需)
- `-g, --group-id`: 消费者组 ID (默认: latency-monitor-group)
- `-k, --use-kafka-timestamp`: 使用 Kafka 内置时间戳 (默认: False)
- `-f, --timestamp-field`: 消息中时间戳字段名 (默认: timestamp)
- `--from-beginning`: 是否从头开始消费 (默认: False，从最新消息开始)
- `-i, --stats-interval`: 统计信息输出间隔（秒）(默认: 10)
- `--no-stats`: 禁用统计信息输出
- `-o, --output`: 日志输出文件 (默认: kafka_latency_YYYYMMDD_HHMMSS.log)
- `-bs, --batch-size`: 消息批处理大小 (默认: 100)
- `-pt, --poll-timeout`: 消费者轮询超时时间（秒）(默认: 0.5)

## 输出说明

脚本运行时会在控制台显示简要信息，并将详细的监控结果写入日志文件。日志文件包含以下信息：

- 监控配置信息
- 每批次消息的处理结果
- 延迟统计信息（平均值、中位数、最小值、最大值）
- 消费速率
- 延迟分布
- 异常高延迟消息的详细信息

## 示例输出

```
统计信息 - 消息总数: 1000, 平均延迟: 45.23ms, 中位数延迟: 38.75ms, 消费速率: 256.78消息/秒

========== 统计信息 ==========
消息总数: 1000
平均延迟: 45.23ms (0.05秒)
中位数延迟: 38.75ms (0.04秒)
最小延迟: 12.45ms (0.01秒)
最大延迟: 532.67ms (0.53秒)
消费速率: 256.78消息/秒
短期消费速率(10秒): 263.45消息/秒
长期消费速率: 256.78消息/秒

延迟分布:
  0-10ms: 0 消息 (0.00%)
  10-50ms: 750 消息 (75.00%)
  50-100ms: 200 消息 (20.00%)
  100-500ms: 49 消息 (4.90%)
  500-1000ms: 1 消息 (0.10%)
  1s+: 0 消息 (0.00%)
==============================
```

## 注意事项

1. 确保消息中包含有效的时间戳信息，或者使用 Kafka 内置时间戳
2. 对于高吞吐量的主题，可能需要调整批处理大小和轮询超时时间
3. 监控工具本身会消耗一定的资源，请在生产环境谨慎使用

## 故障排除

- 如果遇到 "未安装confluent-kafka库" 错误，请运行 `pip install confluent-kafka`
- 如果无法连接到 Kafka 集群，请检查服务器地址和网络连接
- 如果延迟计算异常，请检查时间戳字段是否正确，或尝试使用 Kafka 内置时间戳
