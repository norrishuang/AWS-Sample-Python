#!/bin/bash

# 设置环境变量
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export AWS_REGION=$(aws configure get region)
export MSK_BOOTSTRAP_SERVERS="b-1.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092,b-3.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092,b-2.kafkacluster02.iv8hyq.c8.kafka.us-east-1.amazonaws.com:9092" # 替换为实际的MSK引导服务器地址

# 创建ECR仓库（如果不存在）
aws ecr describe-repositories --repository-names msk-log-generator --region $AWS_REGION > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "创建ECR仓库: msk-log-generator"
  aws ecr create-repository --repository-name msk-log-generator --region $AWS_REGION
fi

# 登录到ECR
echo "登录到ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# 构建Docker镜像
echo "构建Docker镜像..."
docker build -t msk-log-generator:latest .

# 标记镜像
echo "标记镜像..."
docker tag msk-log-generator:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/msk-log-generator:latest

# 推送镜像到ECR
echo "推送镜像到ECR..."
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/msk-log-generator:latest

# 替换K8s部署文件中的占位符
echo "更新Kubernetes部署文件..."
sed -i "s/\${AWS_ACCOUNT_ID}/$AWS_ACCOUNT_ID/g" k8s-deployment.yaml
sed -i "s/\${AWS_REGION}/$AWS_REGION/g" k8s-deployment.yaml
sed -i "s/BOOTSTRAP_SERVERS_PLACEHOLDER/$MSK_BOOTSTRAP_SERVERS/g" k8s-deployment.yaml

# 部署到Kubernetes
echo "部署到Kubernetes..."
kubectl apply -f k8s-deployment.yaml

echo "部署完成！"
echo "检查部署状态: kubectl get pods -l app=msk-log-generator"
