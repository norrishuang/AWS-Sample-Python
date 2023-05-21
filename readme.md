# Python Sample

提供在AWS上Anayltics服务中与Python相关的代码样例。

涉及服务：

1. Glue

   1. Glue支持Datalake

      Glue 3.0/4.0版本已经全面支持数据湖组件（Iceberg/Hudi/DeltaLake）

      - Iceberg
      - Hudi
      - DeltaLake

2. EMR Serverless

   | File                  | 简介                                                       |
   | --------------------- | ---------------------------------------------------------- |
   | **SparkJobSample.py** | EMR Serverless中通过读取hivesql文件，运行spark任务的方法。 |

3. EMR Serverless 数据湖的实现

   EMR 已经实现了支持Hudi/Iceberg/DeltaLake。

   | File                             | 简介                                                         |
   | -------------------------------- | ------------------------------------------------------------ |
   | **Iceberg/CDC_Kafka_Iceberg.py** | pyspark代码实现消费MSK Serverless的 CDC 数据，写入Iceberg。支持多表，支持Schema变更。支持I/U/D。 |