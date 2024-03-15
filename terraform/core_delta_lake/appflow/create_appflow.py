import boto3
from botocore.exceptions import ClientError
import json
import os


client = boto3.client('appflow', region_name='eu-central-1')
config_file_path = f"{os.getcwd()}/appflow/flow_config.json"

def create_flow_from_config(flow_config):
    try:
        response = client.create_flow(**flow_config)
        print("Flow ARN:", response['flowArn'])
        return response['flowArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print("Flow already exists. Skipping creation.")
        else:
            raise e

def activate_flow(flow_name):
    try:
        client.start_flow(flowName=flow_name)
        print("Flow activated successfully.")
    except ClientError as e:
        print(f"Failed to start flow {flow_name}: {e}")

# Load json files
with open(config_file_path) as f:
    flows_config = json.load(f)


service_now_profile = os.getenv('ConnectorProfileName')
landing_bucket_name = os.getenv('LandingBucket')
    

for flow_config in flows_config:
    flow_config['sourceFlowConfig']['connectorProfileName'] = service_now_profile
    for destination_config in flow_config['destinationFlowConfigList']:
        destination_config['destinationConnectorProperties']['S3']['bucketName'] = landing_bucket_name


for flow_config in flows_config:
    flow_arn = create_flow_from_config(flow_config)
    if flow_arn:
        activate_flow(flow_config['flowName'])
