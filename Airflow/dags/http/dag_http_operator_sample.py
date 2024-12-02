from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'http_operator_example',
    default_args=default_args,
    description='A simple DAG with HTTP Operator',
    schedule_interval=timedelta(days=1),
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

http_task >> print_task
