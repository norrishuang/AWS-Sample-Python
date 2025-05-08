#!/bin/bash

# 检查docker-compose是否安装
if ! command -v docker-compose &> /dev/null; then
    echo "docker-compose未安装，正在安装..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.3/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# 构建并启动容器
echo "构建并启动多个日志生成器实例..."
docker-compose up --build -d

# 显示运行状态
echo "容器运行状态:"
docker-compose ps

echo "查看日志:"
echo "docker-compose logs -f"

echo "停止服务:"
echo "docker-compose down"
