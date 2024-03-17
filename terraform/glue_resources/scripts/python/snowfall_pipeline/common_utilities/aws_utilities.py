import boto3
import sys
import json
import zipfile
from awsglue.utils import getResolvedOptions
from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger
from botocore.exceptions import ClientError

class AwsUtilities:

    def __init__(self):
        self.logger = SnowfallLogger.get_logger()

    def get_glue_env_var(self, key, default="none"):
        """Retrieves environment variables specific to AWS Glue.

        Args:
            key (str): The environment variable key.
            default (any): The default value to return if the key is not found.

        Returns:
            str: The value of the environment variable or the default value.
        """
        if f'--{key}' in sys.argv:
            return getResolvedOptions(sys.argv, [key])[key]
        else:
            return default


    def get_workflow_properties(self, key, id=None, name=None):
        """Retrieves properties of a Glue workflow run.

        Args:
            key (str): The key of the property to retrieve.
            id (str, optional): The workflow run ID. Defaults to the ID in environment variable.
            name (str, optional): The workflow name. Defaults to the name in environment variable.

        Returns:
            The value of the requested workflow property.

        Raises:
            Exception: If an error occurs during the API call.
        """
        if id is None:
            id = self.get_glue_env_var('WORKFLOW_RUN_ID')
        if name is None:
            name = self.get_glue_env_var('WORKFLOW_NAME')

        try:
            glue_client = boto3.client('glue')
            response = glue_client.get_workflow_run_properties(Name=name, RunId=id)
            self.logger.info(f"Successfully retrieved workflow properties for the key {key} which is {response['RunProperties'][key]}")
            return response['RunProperties'][key]
        except Exception as e:
            self.logger.error(f"Error in get_workflow_properties: {e}")
            raise e


    def get_workflow_run_data(self, id=None, name=None):
        """Retrieves data of a specific Glue workflow run.

        Args:
            id (str, optional): The workflow run ID. Defaults to the ID in environment variable.
            name (str, optional): The workflow name. Defaults to the name in environment variable.

        Returns:
            dict: The data of the requested workflow run.

        Raises:
            Exception: If an error occurs during the API call.
        """
        if id is None:
            id = self.get_glue_env_var('WORKFLOW_RUN_ID')
        if name is None:
            name = self.get_glue_env_var('WORKFLOW_NAME')

        try:
            glue_client = boto3.client('glue')
            response = glue_client.get_workflow_run(Name=name, RunId=id, IncludeGraph=False)
            self.logger.info("Successfully retrieved workflow run data.")
            return response['Run']
        except Exception as e:
            self.logger.error(f"Error in get_workflow_run_data: {e}")
            raise e


    def get_files_in_s3_path(self, s3_path):
        """Lists files in a specified S3 path.

        Args:
            s3_path (str): The S3 path (e.g., 's3://my-bucket/my-folder/').

        Returns:
            list: A list of file keys found in the specified S3 path.
        """
        # Parse bucket name and prefix from the s3 path
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        bucket_name, prefix = s3_path.split('/', 1)

        s3_client = boto3.client('s3')
        files_list = []
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if not key.endswith('/'):  # Skip directories
                            files_list.append(key)
            self.logger.info("Successfully retrieved files from S3 path.")
        except Exception as e:
            self.logger.error(f"Error in get_files_in_s3_path: {e}")
            return []
        return files_list
    
    
    def check_if_delta_table_exists(self,s3_path):
        """
        Check if a Delta table exists in the specified S3 path.

        Parameters:
        - s3_path (str): The S3 path to check for Delta table existence.

        Returns:
        - bool: True if a Delta table exists, False otherwise.
        """
        # Parse bucket name and prefix from the s3 path
        if s3_path.startswith('s3://'):
            s3_path = s3_path[5:]
        bucket_name, prefix = s3_path.split('/', 1)

        # Initialize AWS S3 client
        s3_client = boto3.client('s3')

        # List objects in the specified S3 path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Check if any object ends with '_delta_log/'
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('_delta_log/'):
                self.logger.info(f"Delta table exists at: {s3_path}")
                return True
        self.logger.info(f"Delta table does not exist at: {s3_path}")
        return False


    def move_s3_object(self, bucket_name, source_object_key, destination_object_key):
        """Moves an object within S3 from one key to another.

        Args:
            bucket_name (str): The name of the S3 bucket.
            source_object_key (str): The key of the source object.
            destination_object_key (str): The key for the object at its new location.

        Returns:
            bool: True if the move operation was successful, False otherwise.
        """
        s3_resource = boto3.resource('s3')
        source_object = s3_resource.Object(bucket_name, source_object_key)
        destination_object = s3_resource.Object(bucket_name, destination_object_key)

        try:
            # Copy the object to the new location
            copy_source = {'Bucket': bucket_name, 'Key': source_object_key}
            destination_object.copy_from(CopySource=copy_source)

            # Delete the original object
            source_object.delete()
            self.logger.info(f"Object moved from '{source_object_key}' to '{destination_object_key}'")

        except ClientError as c:
            if c.response['Error']['Code'] == 'NoSuchKey':
                self.logger.error(f"The source object '{source_object_key}' does not exist in S3.")
            else:
                self.logger.error(f"Error in move_s3_object: {c}")
                raise c
        except Exception as e:
            raise e


    def send_sns_message(self, message, topic_arn=None):
        """Sends a customized message to an SNS topic based on Glue workflow status.

        Args:
            message (str): The base message to send.
            topic_arn (str): The Amazon Resource Name (ARN) of the SNS topic. Defaults to Glue env var.

        Returns:
            dict: The response from the SNS service, or None if an error occurred.
        """
        if topic_arn is None:
            topic_arn = self.get_glue_env_var('SNS_TOPIC_ARN')

        glue_workflow_response = self.get_workflow_run_data()

        try:
            full_message = f"""
            Hi,

            ---------------------------------------------------------------------------------------
            Data Pipeline Error Detected
            ---------------------------------------------------------------------------------------
            Workflow Name :      {self.get_glue_env_var('WORKFLOW_NAME')}
            Workflow Run ID :    {self.get_glue_env_var('WORKFLOW_RUN_ID')}

            StartedOn       :   {glue_workflow_response.get('StartedOn', 'N/A')}
            Status          :   FAILED

            Error Message :      {message}

            Kind Regards,

            UK Snowfall Team
            ------------------------------------------------------------------------------------"""

            sns_client = boto3.client('sns')
            response = sns_client.publish(
                TopicArn=topic_arn,
                Message=full_message,
                Subject=f"Error in {self.get_glue_env_var('WORKFLOW_NAME')}"
            )
            self.logger.info(f"Message sent to SNS topic: {topic_arn}")
            return response
        except Exception as e:
            self.logger.error(f"Error sending message to SNS topic: {e}")
            return None


    def extract_appflow_records_processed(self, filenames, appflow_name):
        """Extracts and sums the number of records processed by AWS AppFlow.

        Args:
            filenames (list[str]): A list of filenames to process.
            appflow_name (str): The name of the AppFlow flow.

        Returns:
            int or None: Sum of records processed, or None if an error occurs or appflow_name is None.
        """
        if appflow_name is None:
            return None
        
        output_list = []
        for filename in filenames:
            parts = filename.split('-')
            if len(parts) >= 3:
                extracted_part = filename.split('/')[-1].rsplit('-', 3)[0]
                output_list.append(extracted_part)
            else:
                return None

        try:
            appflow_client = boto3.client('appflow')
            response = appflow_client.describe_flow_execution_records(
                flowName=appflow_name,
                maxResults=20
            )['flowExecutions']

            num_list = [int(record['executionResult']['recordsProcessed'])
                        for query_id in output_list
                        for record in response if query_id == record['executionId']]

            return sum(num_list) if num_list else None
        except Exception as e:
            self.logger.error(f"Error in extract_appflow_records_processed: {e}")
            return None

    def reading_json_from_zip(self):
        """
        Read JSON data from the zip file which is uploaded to glue.

        Returns:
            dict: JSON data read from the file.
        
        Raises:
            FileNotFoundError: If the specified JSON file is not found in the zip archive.
            json.JSONDecodeError: If the JSON data cannot be decoded.
        """
        # Path to your zip file
        zip_file_path = "snowfall_pipeline.zip"

        # Name of the JSON file inside the zip archive
        json_file_name = "snowfall_pipeline/common_utilities/script_config.json"

        # Open the zip file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_file:
            # Check if the JSON file exists in the zip archive
            if json_file_name in zip_file.namelist():
                # Read the JSON file directly from the zip archive
                with zip_file.open(json_file_name) as json_file:
                    # Load JSON data
                    json_data = json.load(json_file)
                    return json_data
            else:
                raise FileNotFoundError("JSON file not found in the zip archive.")
    


    def create_athena_delta_table(self, database, table_name, s3_path_to_delta, output_location):
        """
        Create an Athena external table for Delta data.

        Parameters:
        - database (str): Name of the database (schema) where the table will be created.
        - table_name (str): Name of the table to be created.
        - s3_path_to_delta (str): S3 location where the Delta data is stored.
        - output_location (str): S3 bucket location where query results will be stored.

        Returns:
        - str: Query execution ID.
        """
        # Determine the full database name
        databases = {
            'raw': 'uk_snowfall_raw',
            'preparation': 'uk_snowfall_preparation',
            'processed': 'uk_snowfall_processed',
            'semantic': 'uk_snowfall_semantic'
        }

        full_database_name = databases.get(database)
        if full_database_name is None:
            self.logger.error('No matching database name found')
            raise Exception('No matching database name found')

        # Initialize Athena client
        client = boto3.client('athena')

        sql_query = f"""CREATE EXTERNAL TABLE IF NOT EXISTS
                    {full_database_name}.{table_name}
                    LOCATION '{s3_path_to_delta}'
                    TBLPROPERTIES ('table_type' = 'DELTA')
                    """

        try:
            # Start query execution
            response = client.start_query_execution(
                QueryString=sql_query,
                ResultConfiguration={
                    'OutputLocation': output_location
                }
            )

            # Extract and return query execution ID
            query_execution_id = response['QueryExecutionId']
            return query_execution_id
        except Exception as e:
            # Log the error and continue
            self.logger.info(f"An error occurred while creating Athena Delta table: {str(e)}")
            return