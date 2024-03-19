# Testing  
import boto3
import os
import urllib.parse
import logging
import json

from snowfall_sources.base_moving import base_moving
from snowfall_sources.amazon_connect import amazon_connect

target_bucket = os.environ.get('TARGET_BUCKET')
sns_arn = os.environ.get('SNS_TOPIC_ARN')
s3 = boto3.client('s3')

# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")


# Load key mapping from a JSON file
def load_key_mapping():
    with open('mapping.json', 'r') as file:
        mappings = json.load(file)
    return mappings.get("fileMappings", {})


def lambda_handler(event, context):
    try:
        # Load the mappings
        key_mapping = load_key_mapping()

        # Get the bucket name and key from the S3 event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote(event['Records'][0]['s3']['object']['key'])

        logger.info(f"Object Key is: {key}")
        target_key = target_key_generator(key, key_mapping)

        if target_key is None:
            logger.info('Folder has been uploaded. Existing Function..')
            return

        logger.info(f"Target Key is: {target_key}")

        if target_key == 'amazon_connect':
            amazon_connect(bucket, key, target_bucket, target_key)
        else:
            # Trigger the functions
            base_moving(bucket, key, target_bucket, target_key)

        logger.info(f'Deleting {key}')
        s3.delete_object(Bucket=bucket, Key=key)
    
    except Exception as e:
        # Handle the error and notify the SNS topic
        s3.copy_object(Bucket=target_bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=f'error/{key}')
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Moved file to: s3://{bucket}/error/{key}")
        error_message = f"Lambda function encountered an error: {str(e)}"
        send_sns_message(error_message)
        raise e


def target_key_generator(key, key_mapping):
    if key.endswith('/'):
        logger.info('Folder uploaded. Skipping...')
        return None  
    
    # Iterate through each dictionary in the list
    for mapping_dict in key_mapping:
        mapping_key = mapping_dict['fileName']
        target_folder = mapping_dict['destinationPath']
        # Check if the current mapping key is a substring of the input key
        if mapping_key in key:
            return target_folder
    
    # If no matching key is found
    raise Exception(f'The object {key} is not recognised')


def send_sns_message(message, topic_arn=os.environ.get('SNS_TOPIC_ARN')):
    sns = boto3.client('sns')
    try:
        response = sns.publish(TopicArn=topic_arn, Message=message, Subject="Error in Landing Bucket")
        logger.info(f"Message sent to SNS topic: {topic_arn}")
        return response
    except Exception as e:
        logger.info(f"Error sending message to SNS topic: {str(e)}")
        return None
