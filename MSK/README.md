# MSK 数据生产者

这个项目用于将样本日志数据循环不断地写入 Amazon MSK (Managed Streaming for Kafka)。日志生成器会持续生成日志文件，并通过 Filebeat 收集发送到 Kafka。
生产者程序运行在 EKS 中，可以指定启动的 Docker 数量来模拟大批量的数据生产。

## 项目结构

```
.
├── log_generator.py      # 日志生成器脚本，支持日志滚动
├── filebeat.yml          # Filebeat 配置文件
├── start.sh              # 容器内启动脚本
├── Dockerfile            # Docker 镜像构建文件
├── docker-compose.yml    # Docker Compose配置文件
├── run-local.sh          # 本地运行脚本
├── k8s-deployment.yaml   # Kubernetes 部署配置
├── build-and-deploy.sh   # 构建和部署脚本
├── requirements.txt      # Python 依赖
└── lazada-log-002.txt    # 样本数据文件
```

## 功能特点

- 持续生成日志文件，模拟真实应用场景
- 支持日志文件滚动，控制磁盘使用
- 每个日志文件最大 100MB，最多保留 10 个文件
- 使用 Filebeat 收集日志并发送到 Kafka
- 支持多行日志合并
- 支持多实例部署，每个实例生成带有唯一标识的日志
- 容器化部署，可在本地Docker或Kubernetes环境运行

## 本地运行多个实例

使用Docker Compose可以在本地轻松运行多个日志生成器实例：

```bash
./run-local.sh
```

这将启动3个日志生成器实例，每个实例都会生成带有唯一标识的日志。

## 部署到 EKS

### 1. 准备工作

确保您已经：
- 安装了 Docker 和 kubectl
- 配置了 AWS CLI 凭证
- 有权限访问 EKS 集群
- 有权限创建和推送 ECR 镜像

### 2. 修改配置

编辑 `build-and-deploy.sh` 文件，设置 MSK 引导服务器地址：

```bash
export MSK_BOOTSTRAP_SERVERS="broker1:9092,broker2:9092,broker3:9092"
```

### 3. 构建和部署

执行构建和部署脚本：

```bash
./build-and-deploy.sh
```

该脚本会：
- 创建 ECR 仓库（如果不存在）
- 构建 Docker 镜像
- 推送镜像到 ECR
- 更新 Kubernetes 部署文件
- 部署到 EKS 集群

### 4. 验证部署

检查部署状态：

```bash
kubectl get pods -l app=msk-log-generator
```

查看日志：

```bash
kubectl logs -f deployment/msk-log-generator
```

## 自定义配置

### Docker Compose配置

您可以通过修改 `docker-compose.yml` 中的 `deploy.replicas` 值来调整实例数量。

### Kubernetes配置

您可以通过修改 `k8s-deployment.yaml` 中的以下内容来自定义配置：

- `spec.replicas`: 部署的实例数量
- 环境变量:
  - `LOG_INTERVAL`: 日志生成间隔（秒）
  - `MAX_LOG_SIZE`: 每个日志文件的最大大小（MB）
  - `BACKUP_COUNT`: 保留的日志文件数量

## 扩展和监控

- 可以通过调整 Deployment 的 `replicas` 值来增加日志生成器的实例数
- 可以使用 Kubernetes 的资源监控工具监控容器的资源使用情况
- 可以配置 Prometheus 和 Grafana 来监控 Filebeat 的指标

## 部署步骤

### 1. 构建 Docker 镜像

```bash
# 设置环境变量
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=$(aws configure get region)

# 创建 ECR 仓库 (如果不存在)
aws ecr create-repository --repository-name msk-data-producer --region $AWS_REGION

# 登录到 ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# 构建镜像
docker build -t msk-data-producer:latest -f msk-producer.Dockerfile .

# 标记镜像
docker tag msk-data-producer:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/msk-data-producer:latest

# 推送镜像到 ECR
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/msk-data-producer:latest
```

### 2. 更新 Kubernetes 部署配置

编辑 `k8s-deployment.yaml` 文件，替换以下占位符：

- `${AWS_ACCOUNT_ID}`: 您的 AWS 账号 ID
- `${AWS_REGION}`: 您的 AWS 区域
- `BOOTSTRAP_SERVERS_PLACEHOLDER`: MSK 集群的引导服务器地址，格式为 `broker1:9092,broker2:9092,broker3:9092`

### 3. 部署到 EKS

```bash
# 确保已配置 kubectl 连接到您的 EKS 集群
aws eks update-kubeconfig --name your-cluster-name --region $AWS_REGION

# 应用部署配置
kubectl apply -f k8s-deployment.yaml

# 检查部署状态
kubectl get pods -l app=msk-data-producer
```

### 4. 监控日志

```bash
# 获取 Pod 名称
POD_NAME=$(kubectl get pods -l app=msk-data-producer -o jsonpath="{.items[0].metadata.name}")

# 查看日志
kubectl logs -f $POD_NAME
```

## 配置选项

通过环境变量可以自定义以下配置：

- `KAFKA_BOOTSTRAP_SERVERS`: MSK 集群的引导服务器地址
- `KAFKA_TOPIC`: Kafka 主题名称 (默认: lazada-logs)
- `DATA_FILE`: 样本数据文件路径 (默认: /app/data/lazada-log-002.txt)
- `INTERVAL_SECONDS`: 消息发送间隔时间，单位秒 (默认: 0.1)

## 安全配置

如果您的 MSK 集群启用了 TLS/SSL 加密，您需要：

1. 创建包含证书的 Kubernetes Secret
2. 取消注释 `k8s-deployment.yaml` 中的 `volumeMounts` 和 `volumes` 部分
3. 修改 `producer.py` 中的 Kafka 生产者配置，启用 SSL 连接
