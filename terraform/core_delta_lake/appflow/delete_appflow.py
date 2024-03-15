import boto3
import json

client = boto3.client('appflow', region_name='eu-central-1')

def delete_flow(flow_name):
    try:
        client.delete_flow(flowName=flow_name,forceDelete=True)
        print(f"Flow '{flow_name}' deleted successfully.")
    except client.exceptions.ResourceNotFoundException:
        print(f"Flow '{flow_name}' not found.")
    except Exception as e:
        print(f"Failed to delete flow '{flow_name}': {e}")

# Load JSON files
with open(f'{os.getcwd()}/flow_config.json') as f:
    flows_config = json.load(f)


for flow_config in flows_config:
    flow_name = flow_config.get('flowName')
    if flow_name:
        delete_flow(flow_name)
    else:
        print("Flow name not found in configuration:", flow_config)
