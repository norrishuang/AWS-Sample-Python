import pytz
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 3, 1,30),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['12380647@qq.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'http_operator_example_external',
    default_args=default_args,
    description='A simple DAG with HTTP Operator',
    schedule_interval='10 3 * * *',

)

def print_response(**context):
    response = context['ti'].xcom_pull(task_ids='http_task')
    print(f"HTTP Response: {response}")
    return response

http_task = SimpleHttpOperator(
    task_id='http_task',
    http_conn_id='conn_http_sample_opensearch_url',
    endpoint='_cat/indices',
    method='GET',
    dag=dag,
)

print_task = PythonOperator(
    task_id='print_task',
    python_callable=print_response,
    provide_context=True,
    dag=dag,
)

def get_execution_date(execution_date, **kwargs):
    # execution_date = context['execution_date']
    # 向前搜索 5 小时
    search_time = execution_date - timedelta(hours=24)
    print(f"Searching for DAG runs at or after: {search_time}")
    return search_time

monitor_external_dag = ExternalTaskSensor(
    task_id='waiting-dag-glue-job',
    external_dag_id='run_existing_glue_job',        # 需要等待的外部DAG id
    external_task_id=None,        # 需要等待的外部Task id
    execution_delta=timedelta(minutes=5),    # 执行时间差，这里指定5分钟，那么当前ExternalTaskSensor会基于当前执行时间（1:05）往前倒5分钟（1:00）寻找在这个时间点依赖 task 已经成功执行完毕
    # execution_date_fn=get_execution_date,
    ## 假如「**init.common.1d**」的执行规则是「10 1 * * *」也就是每天凌晨1点10分，
    ## 那么这里可以使用「execution_date_fn」，让当前DAG等待至1点10分，
    ## 直到「**init.common.1d**」的「save_env_conf」成功执行完
    # execution_date_fn=lambda dt: dt + timedelta(minutes=5),
    timeout=60,                             # 超时时间，如果等待了 60 秒还未符合期望状态的外部Task，那么抛出异常进入重试
    allowed_states=['success'],              # Task允许的状态，这里只允许外部Task执行状态为'success'
    mode='reschedule',                       # reschedule模式，在等待的时候，两次检查期间会sleep当前Task，节约系统开销
    check_existence=True,                    # 校验外部Task是否存在，不存在立马结束等待
    dag=dag,
    poke_interval=30,             # Check every 30 seconds
)

monitor_external_dag >> http_task >> print_task
