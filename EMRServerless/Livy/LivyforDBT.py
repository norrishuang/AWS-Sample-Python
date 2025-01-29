
from botocore import crt
import requests
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import botocore.session
import json, pprint, textwrap
import time

endpoint = 'https://00fkht2eodujab09.livy.emr-serverless-services.us-east-1.amazonaws.com'
headers = {'Content-Type': 'application/json'}

session = botocore.session.Session()
signer = crt.auth.CrtS3SigV4Auth(session.get_credentials(), 'emr-serverless', 'us-east-1')

# check session status
def check_session_status(session_id):
    status_url = f"{endpoint}/sessions/{session_id}/state"
    request = AWSRequest(method='GET', url=status_url, headers=headers)
    request.context["payload_signing_enabled"] = False
    signer.add_auth(request)
    prepped = request.prepare()
    response = requests.get(prepped.url, headers=prepped.headers)
    return response.json()['state']

# check statement status
def check_statement_status(statements_url):
    request = AWSRequest(method='GET', url=statements_url, headers=headers)
    request.context["payload_signing_enabled"] = False
    signer.add_auth(request)
    prepped = request.prepare()
    response = requests.get(prepped.url, headers=prepped.headers)
    # print('check_statement_status response...')
    # print(response.json())
    return response.json()['statements'][0]['state']

### Create session request

data = {'kind': 'pyspark', 'heartbeatTimeoutInSecond': 60, 'conf': { 'emr-serverless.session.executionRoleArn': 'arn:aws:iam::812046859005:role/EMR-Serverless-HMS-EMRServerlessJobRole-16C21VVE86SKI'}}

request = AWSRequest(method='POST', url=endpoint + "/sessions", data=json.dumps(data), headers=headers)
request.context["payload_signing_enabled"] = False
signer.add_auth(request)
prepped = request.prepare()
r = requests.post(prepped.url, headers=prepped.headers, data=json.dumps(data))
pprint.pprint(r.json())


## waiting to get session status  = active
session_id = r.json()['id']

# Wait for the session to become active
while True:
    state = check_session_status(session_id)
    print(f"Session {session_id} is in state: {state}")
    if state in ['idle', 'available']:
        print("Session is active and ready to use.")
        break
    elif state in ['dead', 'error']:
        raise Exception(f"Session {session_id} failed to start with state: {state}")
    time.sleep(10)  # Wait for 10 seconds before checking again


data = {
    'code': textwrap.dedent("""
    from py4j.java_gateway import java_import
    from pyspark.sql import SQLContext
    from pyspark.sql import SparkSession
    import socket
    
    spark = SparkSession.builder.appName("PysparkThriftServer").getOrCreate()
    
    ## start the Thrift Server
    java_import(sc._jvm, "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2")
    sqlCtx = SQLContext(sc)
    ThriftGateway = sc._jvm.HiveThriftServer2.startWithContext(sqlCtx._ssql_ctx)
    
    spark_ui_url = spark.sparkContext.uiWebUrl
    
    print(f'Spark Thrift Server is listening on : {socket.gethostbyname(socket.gethostname())}:10001')
    print(f'Spark UI is available at: {spark_ui_url}')
    """)
}


statements_url = endpoint + r.headers['location'] + "/statements"
request = AWSRequest(method='POST', url=statements_url, data=json.dumps(data), headers=headers)
request.context["payload_signing_enabled"] = False
signer.add_auth(request)
prepped = request.prepare()
r4 = requests.post(prepped.url, headers=prepped.headers, data=json.dumps(data))
pprint.pprint(r4.json())

# Wait for the statement to become available
while True:
    state = check_statement_status(statements_url)
    print(f"Statement is in state: {state}")
    if state in ['available']:
        print("Statement is available and ready to use.")
        break
    elif state in ['error']:
        raise Exception(f"Statement failed with state: {state}")
    time.sleep(10)  # Wait for 10 seconds before checking again


specific_statement_url = endpoint + r4.headers['location']
request = AWSRequest(method='GET', url=specific_statement_url, headers=headers)
request.context["payload_signing_enabled"] = False
signer.add_auth(request)
prepped = request.prepare()
r5 = requests.get(prepped.url, headers=prepped.headers)
pprint.pprint('get result...')
pprint.pprint(r5.json()['output']['data']['text/plain'])



