import boto3
import logging


# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")


s3 = boto3.client('s3')

def base_moving(bucket,key,target_bucket,target_key):
    filename = key.split('/')[-1]
    new_filename = f"{target_key}/{filename}"

    logger.info(f'Copying file from {key} to {new_filename}')
    # Copy the object to the new location within the same bucket
    s3.copy_object(
        Bucket=target_bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=new_filename
    )

    return
