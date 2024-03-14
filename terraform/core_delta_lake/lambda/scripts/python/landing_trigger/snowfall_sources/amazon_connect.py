import datetime
import re
import awswrangler as wr
import logging


# Configure logging
logger = logging.getLogger()
logger.setLevel("INFO")

def amazon_connect(bucket,key,target_bucket,target_key):

    # Extract date from filename
    try:
        match = re.search(r'\d{4}-\d{2}-\d{2}', key)
        if not match:
            raise ValueError("Date not found in filename.")
        
        file_date_str = match.group(0)
        
        # Validate date
        file_date = datetime.datetime.strptime(file_date_str, '%Y-%m-%d').date()
        

        logger.info(f'Reading in s3://{bucket}/{key}')
        df = wr.s3.read_csv(f"s3://{bucket}/{key}")
        df.columns = [col.replace(' ', '_').lower() for col in df.columns]
        logger.info('Adding date column into file....')
        df['file_upload_date'] = file_date_str

        new_filename = key.split('/')[-1]
        output_path = f"s3://{target_bucket}/{target_key}/{new_filename}"

        wr.s3.to_csv(df, path=output_path, index=False)

        logger.info(f'Successfully exported to {output_path}')

    except Exception as e:
        logger.info(e)
        raise e

    return
