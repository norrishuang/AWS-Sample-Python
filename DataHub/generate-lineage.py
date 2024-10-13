from platform import platform

from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)


def datasetUrn(dataType,tbl):
    return builder.make_dataset_urn(dataType, tbl,"PROD")


def fldUrn(dataType,tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(dataType,tbl), fld)

# lineage_emitter_dataset_finegrained_sample.py

# 语法：insert into demo  原始查询语句
# sql = """create table tpcds.dwd_tpcds_q1 as
# select i_item_id,
#         avg(ss_quantity) agg1,
#         avg(ss_list_price) agg2,
#         avg(ss_coupon_amt) agg3,
#         avg(ss_sales_price) agg4
#  from tpcds.store_sales, tpcds.customer_demographics, tpcds.date_dim, tpcds.item, tpcds.promotion
#  where ss_sold_date_sk = d_date_sk and
#        ss_item_sk = i_item_sk and
#        ss_cdemo_sk = cd_demo_sk and
#        ss_promo_sk = p_promo_sk and
#        cd_gender = 'M' and
#        cd_marital_status = 'M' and
#        cd_education_status = '4 yr Degree' and
#        (p_channel_email = 'N' or p_channel_event = 'N') and
#        d_year = 2001
#  group by i_item_id
#  order by i_item_id
#  limit 100;"""

sql = """Create table IF NOT EXISTS tpcds.tpcds_q1_result as
WITH customer_total_return AS  (SELECT customer.sr_customer_sk AS ctr_customer_sk,
store_returns.sr_store_sk AS ctr_store_sk, sum(store_returns.sr_return_amt) AS ctr_total_return FROM tpcds.store_returns,
tpcds.date_dim WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000 GROUP BY sr_customer_sk, sr_store_sk)
SELECT c_customer_id FROM customer_total_return ctr1, tpcds.store, tpcds.customer WHERE ctr1.ctr_total_return >
 (SELECT avg(ctr_total_return)*1.2 FROM customer_total_return ctr2 WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk) AND
 s_store_sk = ctr1.ctr_store_sk AND s_state = 'TN' AND ctr1.ctr_customer_sk = c_customer_sk ORDER BY c_customer_id LIMIT 100"""
# 获取sql血缘
result = LineageRunner(sql, dialect="ansi")
print('=== print result ====')
print(result)

targetTableName = ''
# 获取sql中的下游表名
if len(result.target_tables) > 0:
    targetTableName = result.target_tables[0].__str__()

print('======打印列级血缘结果Begin=========')

# 打印列级血缘结果
result.print_column_lineage()

print('======打印列级血缘结果End=========')

# 获取列级血缘
lineage = result.get_column_lineage

# 字段级血缘list
fineGrainedLineageList = []

# 用于冲突检查的上游list
upStreamsList = []


# 遍历列级血缘
for columnTuples in lineage():
    # 上游list
    upStreamStrList = []

    # 下游list
    downStreamStrList = []

    # 逐个字段遍历
    for column in columnTuples:

        # 元组中最后一个元素为下游表名与字段名，其他元素为上游表名与字段名
        print("###遍历字段###")
        print(column)

        # 遍历到最后一个元素，为下游表名与字段名
        if columnTuples.index(column) == len(columnTuples) - 1:
            downStreamFieldName = column.raw_name.__str__()
            downStreamTableName = column.__str__().replace('.' + downStreamFieldName, '').__str__()

            print('下游表名：' + downStreamTableName)
            print('下游字段名：' + downStreamFieldName)

            downStreamStrList.append(fldUrn("glue",downStreamTableName, downStreamFieldName))
        else:
            upStreamFieldName = column.raw_name.__str__()
            upStreamTableName = column.__str__().replace('.' + upStreamFieldName, '').__str__()

            print('上游表名：' + upStreamTableName)
            print('上游字段名：' + upStreamFieldName)

            upStreamStrList.append(fldUrn("glue",upStreamTableName, upStreamFieldName))

            # 用于检查上游血缘是否冲突
            upStreamsList.append(Upstream(dataset=datasetUrn("glue",upStreamTableName), type=DatasetLineageType.TRANSFORMED))

    fineGrainedLineage = FineGrainedLineage(upstreamType=FineGrainedLineageUpstreamType.DATASET,
                                            upstreams=upStreamStrList,
                                            downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                                            downstreams=downStreamStrList)

    fineGrainedLineageList.append(fineGrainedLineage)

fieldLineages = UpstreamLineage(
    upstreams=upStreamsList, fineGrainedLineages=fineGrainedLineageList
)

lineageMcp = MetadataChangeProposalWrapper(
    entityUrn=datasetUrn("glue", targetTableName),  # 下游表名
    aspect=fieldLineages
)

# 调用datahub REST API
emitter = DatahubRestEmitter('http://54.197.155.121:8080') # datahub server

# Emit metadata!
print(lineageMcp)
emitter.emit_mcp(lineageMcp)

#将表之间血缘关系进一步上传，弥补字段级血缘关系解析来源表少一部分的问题


for target_table in result.target_tables:
    target_table=str(target_table)
    print("目标刷新表=>"+target_table)
    input_tables_urn = []
    for source_table in result.source_tables:
        source_table=str(source_table)
        input_tables_urn.append(builder.make_dataset_urn("glue", source_table))
        print(input_tables_urn)
    lineage_mce = builder.make_lineage_mce(
    input_tables_urn,
    builder.make_dataset_urn("glue", target_table),
    )
    emitter.emit_mce(lineage_mce)
    try:
        emitter.emit_mce(lineage_mce)
        print("添加数仓表 【{}】血缘成功".format(target_table))
    except Exception as e:
        print("添加数仓表 【{}】血缘失败".format(target_table))
        print(e)
        break


# # Datahub grenerate sql lineage with parse_sql_linage
# config = DatahubClientConfig(server="http://54.197.155.121:8080")
# print("---start parse_sql_lineage")
# datahubgraph = DataHubGraph(config)
# result = datahubgraph.parse_sql_lineage(sql=sql_1, platform="redshift", default_db="sample_data_dev")
# print(result)