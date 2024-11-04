from airflow import DAG
# from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
        dag_id="redshift-datahub-lineage",
        start_date=datetime(2024, 8, 28),
        schedule_interval = None,
        tags=['datahub','redshift']) as dag:

    db = 'sample_data_dev'
    schema = 'tpcds'

    tpcds_q3_drop = SQLExecuteQueryOperator(
        task_id='tpcds_q3_truncate',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"truncate table {db}.{schema}.dwd_tpcds_3"
    )

    tpcds_q3_create = SQLExecuteQueryOperator(
        task_id='tpcds_q3_insert',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"""
        INSERT INTO {db}.{schema}.dwd_tpcds_3 
        SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(store_sales.ss_ext_sales_price) sum_agg
         FROM  {db}.{schema}.date_dim dt,
               {db}.{schema}.store_sales,
               {db}.{schema}.item
         WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
           AND store_sales.ss_item_sk = item.i_item_sk
           AND item.i_manufact_id = 128
           AND dt.d_moy=11
         GROUP BY dt.d_year, item.i_brand, item.i_brand_id
         ORDER BY dt.d_year, sum_agg desc, brand_id
         LIMIT 100
            """
    )

    tpcds_q7_drop = SQLExecuteQueryOperator(
        task_id='tpcds_q7_truncate',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"""
        truncate table {db}.{schema}.dwd_tpcds_7
            """
    )

    tpcds_q7_create = SQLExecuteQueryOperator(
        task_id='tpcds_q7_insert',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"""
        INSERT INTO {db}.{schema}.dwd_tpcds_7 
          SELECT item.i_item_id,
                avg(store_sales.ss_quantity) agg1,
                avg(store_sales.ss_list_price) agg2,
                avg(store_sales.ss_coupon_amt) agg3,
                avg(store_sales.ss_sales_price) agg4
         FROM {db}.{schema}.store_sales,
              {db}.{schema}.customer_demographics,
              {db}.{schema}.date_dim,
              {db}.{schema}.item,
              {db}.{schema}.promotion
         WHERE ss_sold_date_sk = d_date_sk AND
               ss_item_sk = i_item_sk AND
               ss_cdemo_sk = cd_demo_sk AND
               ss_promo_sk = p_promo_sk AND
               cd_gender = 'M' AND
               cd_marital_status = 'S' AND
               cd_education_status = 'College' AND
               (p_channel_email = 'N' or p_channel_event = 'N') AND
               d_year = 2000
         GROUP BY i_item_id
         ORDER BY i_item_id LIMIT 100
            """)

    tpcds_q13_truncate = SQLExecuteQueryOperator(
        task_id='tpcds_q13_truncate',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"""
        truncate table {db}.{schema}.dwd_tpcds_13
            """
    )

    tpcds_q13_create = SQLExecuteQueryOperator(
        task_id='tpcds_q13_insert',
        conn_id='redshift_default',
        # region='us-east-1',
        # workgroup_name='workgroup-20240715',
        # database='tpcds_data',
        # secret_arn='arn:aws:secretsmanager:us-east-1:812046859005:secret:prod/redshift/my-serverless-6ciizA',
        sql=f"""
        INSERT INTO {db}.{schema}.dwd_tpcds_13 AS 
                  select /* TPC-DS query91.tpl 0.13 */ 
                    cc_call_center_id Call_Center,
                    cc_name Call_Center_Name,
                    cc_manager Manager,
                    sum(cr_net_loss) Returns_Loss
            from
                    {db}.{schema}.call_center,
                    {db}.{schema}.catalog_returns,
                    {db}.{schema}.date_dim,
                    {db}.{schema}.customer,
                    {db}.{schema}.customer_address,
                    {db}.{schema}.customer_demographics,
                    {db}.{schema}.household_demographics
            where
                    cr_call_center_sk       = cc_call_center_sk
            and     cr_returned_date_sk     = d_date_sk
            and     cr_returning_customer_sk= c_customer_sk
            and     cd_demo_sk              = c_current_cdemo_sk
            and     hd_demo_sk              = c_current_hdemo_sk
            and     ca_address_sk           = c_current_addr_sk
            and     d_year                  = 2002 
            and     d_moy                   = 11
            and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
                    or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
            and     hd_buy_potential like 'Unknown%'
            and     ca_gmt_offset           = -6
            group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
            order by sum(cr_net_loss) desc;
            """
    )

    tpcds_q3_drop >> tpcds_q3_create
    tpcds_q7_drop >> tpcds_q7_create
    tpcds_q13_truncate >> tpcds_q13_create