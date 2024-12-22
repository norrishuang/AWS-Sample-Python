#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    dag_id='run_existing_glue_job',
    default_args=default_args,
    description='Run existing Glue job daily',
    schedule_interval='05 * * * *',
    catchup=False
)


# Run the existing Glue job
run_glue_job = GlueJobOperator(
    task_id='run_glue_job_s3_to_mysql',
    job_name='workflow-s3-to-mysql',  # Replace with your Glue job ID
    region_name='us-east-1',      # Replace with your region
    dag=dag
)


wait_for_completion = GlueJobSensor(
    task_id='wait_for_completion_job_s3_to_mysql',
    job_name='workflow-s3-to-mysql',  # Replace with your Glue job ID
    run_id=run_glue_job.output,
    dag=dag,
    poke_interval=60              # Check status every 60 seconds
)


# Set task dependencies
run_glue_job >> wait_for_completion
