from pyspark.sql import SparkSession
import boto3
import socket
import json
import os

def get_current_vpc():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("VPC Identifier") \
        .getOrCreate()
    
    try:
        # Get the region from environment variables or configuration
        region = os.environ.get('AWS_REGION') or os.environ.get('AWS_DEFAULT_REGION')
        if not region:
            # Try to get region from Spark configuration
            region = spark.sparkContext.getConf().get('spark.executorEnv.AWS_REGION', None)
        
        if not region:
            # Default to a region if not found
            region = 'us-east-1'
        
        # Get the hostname of the current machine
        hostname = socket.gethostname()
        
        # If this is an EC2 instance, the hostname might contain the private IP
        try:
            # Try to extract IP from hostname or get local IP
            ip_address = socket.gethostbyname(hostname)
            
            # Use EC2 API to find instances with this private IP
            ec2 = boto3.client('ec2', region_name=region)
            response = ec2.describe_instances(
                Filters=[{'Name': 'private-ip-address', 'Values': [ip_address]}]
            )
            
            if response['Reservations'] and len(response['Reservations']) > 0:
                vpc_id = response['Reservations'][0]['Instances'][0]['VpcId']
                print(f"Current VPC ID: {vpc_id}")
                return vpc_id
        except Exception as e:
            print(f"Could not get VPC from instance IP: {str(e)}")
    
        # For EMR Serverless, try to get VPC from job run details
        try:
            # Get the application ID and job run ID from environment variables
            application_id = os.environ.get('AWS_EMR_SERVERLESS_APPLICATION_ID')
            job_run_id = os.environ.get('AWS_EMR_SERVERLESS_JOB_RUN_ID')
            
            if application_id and job_run_id:
                emr_serverless = boto3.client('emr-serverless', region_name=region)
                job_run = emr_serverless.get_job_run(
                    applicationId=application_id,
                    jobRunId=job_run_id
                )
                
                # The network configuration contains the VPC info
                network_config = job_run.get('networkConfiguration', {})
                vpc_id = network_config.get('vpcId')
                
                if vpc_id:
                    print(f"Current VPC ID (from EMR Serverless): {vpc_id}")
                    return vpc_id
        except Exception as e:
            print(f"Could not get VPC from EMR Serverless job info: {str(e)}")
            
    except Exception as e:
        print(f"Error determining VPC: {str(e)}")
    
    print("Could not determine current VPC")
    return None

if __name__ == "__main__":
    vpc_id = get_current_vpc()
    print(f"Program is running in VPC: {vpc_id}")