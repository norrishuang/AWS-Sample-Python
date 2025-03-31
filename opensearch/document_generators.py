import ijson
import json
import io

def document_generator_with_checkpoint(s3_stream, transform_mongodb_json, extract_mongodb_id, batch_size, index_name, start_position=0):
    """流式读取 S3 中的 JSON 文件并生成文档，支持从指定位置开始处理"""
    # 处理数组格式的 JSON
    objects = ijson.items(s3_stream, 'item')

    batch = []
    position = 0
    
    for i, doc in enumerate(objects):
        position = i + 1  # 当前处理的文档位置（从1开始）
        
        # 如果指定了起始位置，跳过已处理的文档
        if position < start_position:
            continue
            
        # 转换MongoDB格式的JSON，去除$numberInt等包装
        doc = transform_mongodb_json(doc)
        
        # 从MongoDB格式中提取ID
        doc_id = extract_mongodb_id(doc)
        if doc_id is None:
            doc_id = doc.get("_id", str(i))  # 使用普通_id或生成新ID
        
        # 创建文档的副本，以便我们可以安全地修改它
        doc_copy = doc.copy()
        
        # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
        if "_id" in doc_copy:
            del doc_copy["_id"]
        
        # 准备文档
        action = {
            "_index": index_name,
            "_id": doc_id,
            "_source": doc_copy
        }

        batch.append(action)

        # 当批次达到指定大小时，yield 批次并重置
        if len(batch) >= batch_size:
            yield batch, position
            batch = []

    # 处理最后一个不完整的批次
    if batch:
        yield batch, position

def process_non_array_json_with_checkpoint(s3_stream, transform_mongodb_json, extract_mongodb_id, batch_size, index_name, start_position=0):
    """处理非数组格式的 JSON 文件，支持从指定位置开始处理"""
    # 使用 ijson 流式解析顶层键
    parser = ijson.parse(s3_stream)

    current_key = None
    current_doc = {}
    doc_count = 0
    position = 0
    batch = []

    for prefix, event, value in parser:
        # 根据 JSON 结构调整此逻辑
        if event == 'map_key':
            current_key = value
        elif prefix.count('.') == 0 and current_key:  # 顶层键值对
            current_doc[current_key] = value
            current_key = None
        elif event == 'end_map' and prefix == '':  # 文档结束
            if current_doc:
                position += 1  # 当前处理的文档位置（从1开始）
                
                # 如果指定了起始位置，跳过已处理的文档
                if position < start_position:
                    current_doc = {}
                    continue
                
                # 转换MongoDB格式的JSON，去除$numberInt等包装
                current_doc = transform_mongodb_json(current_doc)
                
                # 从MongoDB格式中提取ID
                doc_id = extract_mongodb_id(current_doc)
                if doc_id is None:
                    doc_id = current_doc.get("_id", str(doc_count))
                
                # 创建文档的副本，以便我们可以安全地修改它
                doc_copy = current_doc.copy()
                
                # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
                if "_id" in doc_copy:
                    del doc_copy["_id"]
                
                action = {
                    "_index": index_name,
                    "_id": doc_id,
                    "_source": doc_copy
                }
                batch.append(action)
                doc_count += 1
                current_doc = {}

                if len(batch) >= batch_size:
                    yield batch, position
                    batch = []

    # 处理最后一个批次
    if batch:
        yield batch, position

def process_jsonl_format_with_checkpoint(s3_stream, transform_mongodb_json, extract_mongodb_id, batch_size, index_name, start_position=0):
    """处理JSON Lines格式（每行一个JSON对象），支持从指定位置开始处理"""
    batch = []
    doc_count = 0
    position = 0
    
    # 将S3流转换为文本行
    text_stream = io.TextIOWrapper(s3_stream, encoding='utf-8')
    
    # 按行读取
    for line_number, line in enumerate(text_stream, 1):
        position = line_number  # 当前处理的行号（从1开始）
        
        # 如果指定了起始位置，跳过已处理的行
        if position < start_position:
            continue
            
        if not line.strip():  # 跳过空行
            continue
            
        try:
            # 解析单行JSON
            doc = json.loads(line)
            
            # 转换MongoDB格式的JSON，去除$numberInt等包装
            doc = transform_mongodb_json(doc)
            
            # 从MongoDB格式中提取ID
            doc_id = extract_mongodb_id(doc)
            if doc_id is None:
                doc_id = doc.get("_id", str(doc_count))
                doc_count += 1
            
            # 创建文档的副本，以便我们可以安全地修改它
            doc_copy = doc.copy()
            
            # 从文档中移除_id字段，因为它是OpenSearch的元数据字段
            if "_id" in doc_copy:
                del doc_copy["_id"]
            
            # 准备文档
            action = {
                "_index": index_name,
                "_id": doc_id,
                "_source": doc_copy
            }
            
            batch.append(action)
            
            # 当批次达到指定大小时，yield 批次并重置
            if len(batch) >= batch_size:
                yield batch, position
                batch = []
                
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON at line {line_number}: {e}")
            print(f"Problematic line: {line[:100]}...")  # 只打印前100个字符
            continue  # 跳过这一行，继续处理
    
    # 处理最后一个批次
    if batch:
        yield batch, position
