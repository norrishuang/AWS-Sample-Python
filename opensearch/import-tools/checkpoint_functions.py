import json
import traceback
from botocore.exceptions import ClientError

def save_checkpoint(s3_client, bucket, key, checkpoint_data):
    """
    保存检查点信息到S3
    
    Args:
        s3_client: S3客户端
        bucket: S3桶名称
        key: S3对象键
        checkpoint_data: 检查点数据字典
    """
    try:
        # 将检查点数据转换为JSON字符串
        checkpoint_json = json.dumps(checkpoint_data)
        
        # 上传到S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=checkpoint_json,
            ContentType='application/json'
        )
        
        print(f"Checkpoint saved to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"Error saving checkpoint to S3: {e}")
        traceback.print_exc()
        return False

def load_checkpoint(s3_client, bucket, key):
    """
    从S3加载检查点信息
    
    Args:
        s3_client: S3客户端
        bucket: S3桶名称
        key: S3对象键
        
    Returns:
        检查点数据字典，如果不存在则返回None
    """
    try:
        # 从S3获取检查点文件
        response = s3_client.get_object(Bucket=bucket, Key=key)
        checkpoint_json = response['Body'].read().decode('utf-8')
        
        # 解析JSON
        checkpoint_data = json.loads(checkpoint_json)
        
        print(f"Checkpoint loaded from s3://{bucket}/{key}")
        print(f"Resuming from position: {checkpoint_data.get('position', 0)}, processed: {checkpoint_data.get('processed_docs', 0)} documents")
        
        return checkpoint_data
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"No checkpoint found at s3://{bucket}/{key}")
        else:
            print(f"Error loading checkpoint from S3: {e}")
        return None
    except Exception as e:
        print(f"Error loading checkpoint from S3: {e}")
        traceback.print_exc()
        return None
