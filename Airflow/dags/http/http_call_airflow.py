import logging
import sys
import time
from datetime import datetime

import boto3
import requests

env_name = "MyAirflowEnvironment"

# DAG_ID = "http_operator_example"


def get_session_info(region, env_name):
    logging.basicConfig(level=logging.INFO)

    try:
        # Initialize MWAA client and request a web login token
        mwaa = boto3.client('mwaa', region_name=region)
        response = mwaa.create_web_login_token(Name=env_name)
        
        # Extract the web server hostname and login token
        web_server_host_name = response["WebServerHostname"]
        web_token = response["WebToken"]
        
        # Construct the URL needed for authentication 
        login_url = f"https://{web_server_host_name}/aws_mwaa/login"
        login_payload = {"token": web_token}

        # Make a POST request to the MWAA login url using the login payload
        response = requests.post(
            login_url,
            data=login_payload,
            timeout=10
        )

        # Check if login was succesfull 
        if response.status_code == 200:
        
            # Return the hostname and the session cookie 
            return (
                web_server_host_name,
                response.cookies["session"]
            )
        else:
            # Log an error
            logging.error("Failed to log in: HTTP %d", response.status_code)
            return None
    except requests.RequestException as e:
         # Log any exceptions raised during the request to the MWAA login endpoint
        logging.error("Request failed: %s", str(e))
        return None
    except Exception as e:
        # Log any other unexpected exceptions
        logging.error("An unexpected error occurred: %s", str(e))
        return None

# 获取任务实例的 XCom 值
def get_xcom_value(dag_id, dag_run_id, task_id):
    try:
        web_server_host_name, session_cookie = get_session_info(region, env_name)
        if not session_cookie:
            logging.error("Authentication failed, no session cookie retrieved.")
            return
    except Exception as e:
        logging.error(f"Error retrieving session info: {str(e)}")
        return

    # Prepare headers and payload for the request
    cookies = {"session": session_cookie}

    # Construct the URL for retrieving XCom value
    url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/return_value"

    try:
        response = requests.get(url, cookies=cookies)
        # Check the response status code to determine if the XCom value was retrieved successfully
        if response.status_code == 200:
            return response.json()["value"]
        else:
            logging.error(f"Failed to retrieve XCom value: HTTP {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request to retrieve XCom value failed: {str(e)}")

# 获取 DAG 运行状态
def get_dag_run_status(dag_id, dag_run_id):
    try:
        web_server_host_name, session_cookie = get_session_info(region, env_name)
        if not session_cookie:
            logging.error("Authentication failed, no session cookie retrieved.")
            return
    except Exception as e:
        logging.error(f"Error retrieving session info: {str(e)}")
        return

    # Prepare headers for the request
    cookies = {"session": session_cookie}

    url = f"https://{web_server_host_name}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    response = requests.get(url, cookies=cookies)
    if response.status_code == 200:
        return response.json()['state']
    else:
        raise Exception(f"Failed to get DAG run status: {response.text}")
    
def trigger_dag(region, env_name, dag_name):
    """
    Triggers a DAG in a specified MWAA environment using the Airflow REST API.

    Args:
    region (str): AWS region where the MWAA environment is hosted.
    env_name (str): Name of the MWAA environment.
    dag_name (str): Name of the DAG to trigger.
    """

    logging.info(f"Attempting to trigger DAG {dag_name} in environment {env_name} at region {region}")

    # Retrieve the web server hostname and session cookie for authentication
    try:
        web_server_host_name, session_cookie = get_session_info(region, env_name)
        if not session_cookie:
            logging.error("Authentication failed, no session cookie retrieved.")
            return
    except Exception as e:
        logging.error(f"Error retrieving session info: {str(e)}")
        return

    # Prepare headers and payload for the request
    cookies = {"session": session_cookie}
    json_body = {"conf": {}}

    # Construct the URL for triggering the DAG
    url = f"https://{web_server_host_name}/api/v1/dags/{dag_name}/dagRuns"

    # Send the POST request to trigger the DAG
    try:
        response = requests.post(url, cookies=cookies, json=json_body)
        # Check the response status code to determine if the DAG was triggered successfully
        if response.status_code == 200:
            logging.info("DAG triggered successfully.")
            return response.json()
        else:
            logging.error(f"Failed to trigger DAG: HTTP {response.status_code} - {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request to trigger DAG failed: {str(e)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # record the program's process time.
    start_time = datetime.now()
    print(f"Program started at {start_time}")

    #
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 4:
        logging.error("Incorrect usage. Proper format: python script_name.py {region} {env_name} {dag_name}")
        sys.exit(1)

    region = sys.argv[1]
    env_name = sys.argv[2]
    dag_name = sys.argv[3]

    # Trigger the DAG with the provided arguments
    dag_run = trigger_dag(region, env_name, dag_name)
    dag_run_id = dag_run['dag_run_id']

    print(f"DAG run ID: {dag_run_id}")

    # 等待 DAG 运行完成
    while True:
        status = get_dag_run_status(dag_name, dag_run_id)
        print(f"DAG run status: {status}")
        if status in ['success', 'failed']:
            break
        time.sleep(10)  # 每10秒检查一次状态

    # 获取 HTTP Operator 的返回值
    xcom_value = get_xcom_value(dag_name, dag_run_id, 'http_task')
    if xcom_value:
        print(f"HTTP Operator result: {xcom_value}")
    else:
        print("No XCom value found for HTTP Operator")

    end_time = datetime.now()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time}")