import argparse

import redshift_connector
from sqllineage.runner import LineageRunner
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)

## Describe [generate-lineage-redshift]
## Create 2024-08-29
## Generate the lineage by redshift sql

def unescape_query(query):
    return bytes(query, "utf-8").decode("unicode_escape")

def datasetUrn(dataType,tbl):
    return builder.make_dataset_urn(dataType, tbl,"PROD")


def fldUrn(dataType,tbl, fld):
    return builder.make_schema_field_urn(datasetUrn(dataType,tbl), fld)


def parse_query(query, datahub_api_url):
    query_txt = query["query_txt"]
    try:
        # extract lineage
        # get lineage from sql
        result = LineageRunner(query_txt, dialect="ansi")
        print('=== print result ====')
        print(result)

        targetTableName = ''
        # Get downstream table name
        if len(result.target_tables) > 0:
            targetTableName = result.target_tables[0].__str__()

        print('======print lineage of columns Begin=========')
        # 打印列级血缘结果
        result.print_column_lineage()

        print('======print lineage of columns End=========')

        # 获取列级血缘
        lineage = result.get_column_lineage

        # 字段级血缘list
        fineGrainedLineageList = []

        # 用于冲突检查的上游list
        upStreamsList = []


        # 遍历列级血缘
        for columnTuples in lineage():
            upStreamStrList = []
            downStreamStrList = []
            # get columns
            for column in columnTuples:

                # 元组中最后一个元素为下游表名与字段名，其他元素为上游表名与字段名
                # 遍历到最后一个元素，为下游表名与字段名
                if columnTuples.index(column) == len(columnTuples) - 1:
                    downStreamFieldName = column.raw_name.__str__()
                    downStreamTableName = column.__str__().replace('.' + downStreamFieldName, '').__str__()

                    print('DownStream Table Name:' + downStreamTableName)
                    print('DownStream Filed Name:' + downStreamFieldName)

                    downStreamStrList.append(fldUrn("redshift",downStreamTableName, downStreamFieldName))
                else:
                    upStreamFieldName = column.raw_name.__str__()
                    upStreamTableName = column.__str__().replace('.' + upStreamFieldName, '').__str__()

                    print('UpStream Table Name:' + upStreamTableName)
                    print('UpStream Filed Name:' + upStreamFieldName)

                    upStreamStrList.append(fldUrn("redshift",upStreamTableName, upStreamFieldName))

                    # 用于检查上游血缘是否冲突
                    upStreamsList.append(Upstream(dataset=datasetUrn("redshift",upStreamTableName), type=DatasetLineageType.TRANSFORMED))

            fineGrainedLineage = FineGrainedLineage(upstreamType=FineGrainedLineageUpstreamType.DATASET,
                                                    upstreams=upStreamStrList,
                                                    downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
                                                    downstreams=downStreamStrList)

            fineGrainedLineageList.append(fineGrainedLineage)

        fieldLineages = UpstreamLineage(
            upstreams=upStreamsList, fineGrainedLineages=fineGrainedLineageList
        )

        lineageMcp = MetadataChangeProposalWrapper(
            entityUrn=datasetUrn("redshift", targetTableName),  # 下游表名
            aspect=fieldLineages
        )

        # 调用datahub REST API
        emitter = DatahubRestEmitter(datahub_api_url) # datahub server

        # Emit metadata!
        emitter.emit_mcp(lineageMcp)
    except Exception as e:
        print(f"Failed to parse SQL: {query_txt}, error: {e}")
        return



def print_identity(session):
    # Print information about the caller's identity if the caller has
    # permission to call: iam.list_account_aliases, sts.get_caller_identity.
    try:
        iam_client = session.client(service_name="iam")
        account_alias = iam_client.list_account_aliases()["AccountAliases"][0]
    except Exception:
        account_alias = "-"
    try:
        sts_client = session.client(service_name="sts")
        caller_identity = sts_client.get_caller_identity()
        account_id = caller_identity["Account"]
        user_id = caller_identity["UserId"]
        user_arn = caller_identity["Arn"]
    except Exception:
        account_id = user_id = user_arn = "-"
    print("  IAM identity:\n")
    print(f"    Account alias: {account_alias}")
    print(f"    Account Id:    {account_id}")
    print(f"    User Id:       {user_id}")
    print(f"    ARN:           {user_arn}")

def verify_identity_and_settings(
        datahub_api_url,
        host_name,
        port,
        database_name,
        start_time,
        user,
        password,
):


    print(f"\n  Data Hub Url: {datahub_api_url}\n")

    print("\n  Extracting Amazon Redshift data lineage from:\n")
    print(f"    Host:       {host_name}")
    print(f"    Port:       {port}")
    print(f"    Database:   {database_name}")
    if start_time is None:
        print(f"    Start time: {start_time} - All queries will be processed.")
    else:
        print(
            f"    Start time: {start_time} - Only queries started on or after this time will be processed."
        )
    print(f"    User:       {user}")
    print(f"    Password:   {'<provided>' if password else '<not provided>'}")

    print("\n  Posting data lineage to DataHub:\n")

    user_input = input("\nAre the settings above correct? (yes/no): ")
    if not user_input.lower() == "yes":
        print(f'Exiting. You entered "{user_input}", enter "yes" to continue.')
        exit(0)

def extract_queries_and_post_lineage(
        datahub_api_url,
        host_name,
        port,
        database_name,
        start_time,
        user,
        password,
):
    conn = redshift_connector.connect(
        host=host_name, database=database_name, port=port, user=user, password=password
    )

    cursor = conn.cursor()

    query = """
    SELECT DISTINCT sti.database AS database, sti.schema AS schema, sti.table AS table, sti.table_type,
           qh.user_id AS user_id, qh.query_id AS query_id, qh.transaction_id AS transaction_id,
           qh.session_id AS session_id, qh.start_time AS start_time, qh.end_time AS end_time,
           query_txt
    FROM SYS_QUERY_HISTORY qh
    -- concatenate query_text if it's length > 4K characters
    LEFT JOIN (
        SELECT query_id, LISTAGG(RTRIM(text)) WITHIN GROUP (ORDER BY sequence) AS query_txt
        FROM sys_query_text
        WHERE sequence < 320
        GROUP BY query_id
    ) qt ON qh.query_id = qt.query_id
    -- to get table_id
    LEFT JOIN sys_query_detail qd ON qh.query_id = qd.query_id
    -- get table details
    JOIN (
        SELECT database, schema, table_id, "table", table_type
        FROM svv_table_info sti
        INNER JOIN svv_tables st ON sti.table = st.table_name
        AND sti.database = st.table_catalog
        AND sti.schema = st.table_schema
    ) sti ON qd.table_id = sti.table_id
    WHERE query_type IN ('DDL', 'CTAS', 'COPY', 'INSERT', 'UNLOAD')
    """

    if start_time:
        query = query + f" AND qh.start_time >= '{start_time}'"

    cursor.execute(query)
    results = cursor.fetchall()

    if not results:
        print("\n  No Redshift queries found, no data lineage to extract.\n")
        return

    print(f"\n  Found {len(results)} Redshift {'query' if len(results) == 1 else 'queries'}.\n")

    for res in results:
        query_result = {
            "database": res[0].strip(),
            "schema": res[1].strip(),
            "table": res[2].strip(),
            "table_type": res[3].strip(),
            "user_id": res[4],
            "query_id": res[5],
            "transaction_id": res[6],
            "session_id": res[7],
            "start_time": res[8].isoformat()[:-3] + "Z",
            "end_time": res[9].isoformat()[:-3] + "Z",
            "query_txt": unescape_query(res[10].strip()),
        }
        print("\n  Processing data lineage for query: {query_result['query_txt']}")
        #

        parse_query(query_result, datahub_api_url)

    cursor.close()
    conn.close()

def parse_arguments():
    parser = argparse.ArgumentParser(description="Post Amazon Redshift data lineage to DataHub.")

    parser.add_argument("-l", "--datahub-api-url", help="The DataHub API url.", required=True)

    parser.add_argument("-n", "--host-name", help="The Amazon Redshift host name.", required=True)

    parser.add_argument(
        "-t", "--port", help="The Amazon Redshift host port number.", required=False, default="5439"
    )
    parser.add_argument(
        "-d", "--database-name", help="The Amazon Redshift database name.", required=True
    )
    parser.add_argument(
        "-s",
        "--start-time",
        help='The start time for data lineage extraction. Supported formats: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" (UTC)',
    )
    parser.add_argument(
        "-u", "--user", help="The Amazon Redshift connection user name.", required=True
    )
    parser.add_argument(
        "-w", "--password", help="The Amazon Redshift connection password.", required=True
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    # session = boto3.Session(profile_name=args.profile, region_name=args.region)
    verify_identity_and_settings(
        datahub_api_url=args.datahub_api_url,
        host_name=args.host_name,
        port=args.port,
        database_name=args.database_name,
        start_time=args.start_time,
        user=args.user,
        password=args.password,
    )
    extract_queries_and_post_lineage(
        datahub_api_url=args.datahub_api_url,
        host_name=args.host_name,
        port=args.port,
        database_name=args.database_name,
        start_time=args.start_time,
        user=args.user,
        password=args.password,
    )
