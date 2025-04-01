#!/bin/bash

# 定义颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# 定义EC2密钥文件路径
KEY_FILE=~/ec2-us-east-1.pem

# 检查密钥文件是否存在
if [ ! -f "$KEY_FILE" ]; then
    echo -e "${RED}错误: EC2密钥文件 $KEY_FILE 不存在${NC}"
    exit 1
fi

# 确保密钥文件权限正确
chmod 400 "$KEY_FILE"

# 定义IP列表文件
OPENSEARCH_IPS="opensearch_ips.txt"
ELASTICSEARCH_IPS="elasticsearch_ips.txt"

# 检查IP列表文件是否存在
if [ ! -f "$OPENSEARCH_IPS" ]; then
    echo -e "${YELLOW}警告: OpenSearch IP列表文件 $OPENSEARCH_IPS 不存在${NC}"
    echo -e "${YELLOW}创建示例文件 $OPENSEARCH_IPS${NC}"
    echo "# 每行一个IP地址" > "$OPENSEARCH_IPS"
    echo "# 例如:" >> "$OPENSEARCH_IPS"
    echo "# 10.0.0.1" >> "$OPENSEARCH_IPS"
    echo "# 10.0.0.2" >> "$OPENSEARCH_IPS"
fi

if [ ! -f "$ELASTICSEARCH_IPS" ]; then
    echo -e "${YELLOW}警告: Elasticsearch IP列表文件 $ELASTICSEARCH_IPS 不存在${NC}"
    echo -e "${YELLOW}创建示例文件 $ELASTICSEARCH_IPS${NC}"
    echo "# 每行一个IP地址" > "$ELASTICSEARCH_IPS"
    echo "# 例如:" >> "$ELASTICSEARCH_IPS"
    echo "# 10.0.0.3" >> "$ELASTICSEARCH_IPS"
    echo "# 10.0.0.4" >> "$ELASTICSEARCH_IPS"
fi

# 定义SSH用户名 (默认为ec2-user，可根据需要修改)
SSH_USER="ec2-user"

# 重启OpenSearch服务
echo -e "${GREEN}开始重启 OpenSearch 服务...${NC}"
while IFS= read -r ip || [[ -n "$ip" ]]; do
    # 跳过注释行和空行
    [[ "$ip" =~ ^#.*$ || -z "$ip" ]] && continue
    
    echo -e "${YELLOW}正在连接到 $ip 重启 OpenSearch 服务...${NC}"
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$SSH_USER@$ip" "sudo systemctl restart opensearch" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 成功重启 OpenSearch 服务在 $ip${NC}"
    else
        echo -e "${RED}✗ 无法重启 OpenSearch 服务在 $ip${NC}"
    fi
done < "$OPENSEARCH_IPS"

# 重启Elasticsearch服务
echo -e "${GREEN}开始重启 Elasticsearch 服务...${NC}"
while IFS= read -r ip || [[ -n "$ip" ]]; do
    # 跳过注释行和空行
    [[ "$ip" =~ ^#.*$ || -z "$ip" ]] && continue
    
    echo -e "${YELLOW}正在连接到 $ip 重启 Elasticsearch 服务...${NC}"
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$SSH_USER@$ip" "sudo systemctl restart elasticsearch.service" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ 成功重启 Elasticsearch 服务在 $ip${NC}"
        
        # 启动 Prometheus exporter
        echo -e "${YELLOW}正在启动 Prometheus exporter 在 $ip...${NC}"
        ssh -i "$KEY_FILE" -o StrictHostKeyChecking=no -o ConnectTimeout=10 "$SSH_USER@$ip" "cd /home/ec2-user/elasticsearch_exporter-1.8.0.linux-amd64/ && nohup ./elasticsearch_exporter --web.listen-address=\":9114\" --es.uri http://localhost:9200 > /dev/null 2>&1 &" 2>/dev/null
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ 成功启动 Prometheus exporter 在 $ip${NC}"
        else
            echo -e "${RED}✗ 无法启动 Prometheus exporter 在 $ip${NC}"
        fi
    else
        echo -e "${RED}✗ 无法重启 Elasticsearch 服务在 $ip${NC}"
    fi
done < "$ELASTICSEARCH_IPS"

echo -e "${GREEN}所有服务重启操作已完成${NC}"
