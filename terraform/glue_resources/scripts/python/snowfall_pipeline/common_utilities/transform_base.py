from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger
from snowfall_pipeline.common_utilities.aws_utilities import AwsUtilities
from snowfall_pipeline.common_utilities.decorators import transformation_timer


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import *


from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import SelectFromCollection
from awsgluedq.transforms import EvaluateDataQuality

import re
import random
import string
from datetime import datetime  



class TransformBase:
    """Initialise and configure the transform base.

    This class is used as a base for transformations that occur.

    Attributes:
        logger (Logger): The logger for logging information.
        spark (SparkSession): The Spark session.
        sc (SparkContext): The Spark context.
        glueContext (GlueContext): The Glue context.
        aws_instance (class instance): Initialise the AwsUtilities class so that the methods are available to use instead of having to import for each class
        sns_trigger (bool) : Checks whethers the SNS notification needs to be triggered
        athena_trigger (bool) : Checks whethers athena table needs to be created
        list_of_files : Currently set at None, but when each class inherits this it will set a value. Set here so I can use logic to move files
        account_number (str): Account number that we utilize in AWS used to generate the bucket names.
        environment (str): Environment that we utilize in AWS used to generate the bucket names.
        full_config (dict): Full configuration read from the script_config file in the common_utilities folders.
        raw_bucket_name (str): Name of the raw bucket.
        preparation_bucket_name (str): Name of the preparation bucket.
        processed_bucket_name (str): Name of the processed bucket.
        semantic_bucket_name (str): Name of the semantic bucket.
        datasets (str) : Dataset name pulled from glue workflow propeties
    """

    def __init__(self,spark,sc,glueContext):
        self.logger = SnowfallLogger.get_logger()
        self.spark = spark
        self.sc = sc
        self.glueContext = glueContext
        self._initial_message_printed()
        self.aws_instance = AwsUtilities()
        self.sns_trigger = False
        self.athena_trigger = False
        self.list_of_files = None
        self.account_number = self.aws_instance.get_glue_env_var('ACCOUNT_NUMBER')
        self.environment = self.aws_instance.get_glue_env_var('ENVIRONMENT')
        self.full_configs = self.aws_instance.reading_json_from_zip()
        self.raw_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-raw-{self.account_number}"
        self.preparation_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-preparation-{self.account_number}"
        self.processed_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-processed-{self.account_number}"
        self.semantic_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-semantic-{self.account_number}"
        self.athena_output_path = f"eu-central1-{self.environment}-uk-snowfall-athena-{self.account_number}/"
        self.datasets = self.aws_instance.get_workflow_properties('DATASET')



    def get_data(self):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def transform_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def save_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass


    def process_flow(self):
        "Abstract method which runs each pipeline in order"
        try:
            df = self.get_data()
            transformed_df = self.transform_data(df)
            self.save_data(transformed_df)
        except Exception as e:
            if 'Preparation' in self.__class__.__name__:
                for i in self.list_of_files:
                    self.aws_instance.move_s3_object(self.raw_bucket_name, i, f"error/{i}") 
                    self.aws_instance.send_sns_message(e)
                raise e
            else:
                self.aws_instance.send_sns_message(e)
                raise e



    def _initial_message_printed(self):
        """Prints an initial message with hyphens for the pipeline which is running
        """
        message = f"Running the {self.__class__.__name__} Pipeline"
        hyphen_length = 30
        seperator = '-' * hyphen_length
        formatted_message = f"{seperator}{message.center(hyphen_length)}{seperator}"
        self.logger.info(formatted_message)


    def get_column_count_from_config(self, input_string):
        """Extract the expected column count from the data quality rules which are in the config files.

        Args:
            input_string (str): The data quality rules string.

        Returns:
            int: The expected column count for a particular dataset.

        Raises:
            Exception: If no column count number is found in the data quality rules.
        """
        pattern = r'ColumnCount\s*=\s*(\d+)'
        match = re.search(pattern, input_string)
        # Plus 4 since there are 4 DQ columns
        if match:
            column_count = int(match.group(1)) + 4
            return column_count
        else:
            raise Exception("No Column Count number in Data Quality Rules")   


    @transformation_timer
    def data_quality_check(self, df, dq_rules, pk, bucket_name, s3_path_prefix, output_file_type):

        """Perform data quality checks on the DataFrame and return the passed rows.

        Args:
            df (DataFrame): The input DataFrame to perform data quality checks on.
            dq_rules (string): The string from config files regarding the dq rules.
            pk (string): The name of the primary key column that you want to check for non null
            bucket_name (string): The name of the S3 bucket.
            s3_path_prefix (string): The prefix of the S3 path.
            output_file_type (string): The type of output file.

        Returns:
            DataFrame: The DataFrame containing rows that passed the data quality checks.
            (Note that 4 extra columns are added due to this data quality check)

        Raises:
            Exception: If there are issues with data quality checks or loading the source data.
        """
        self.logger.info('Running the data quality check ......')
        self.logger.info(f'Rules to be applied on the data: {dq_rules}')

        column_count_expected = self.get_column_count_from_config(dq_rules)

        dyf = DynamicFrame.fromDF(df,self.glueContext, "dyf")

        EvaluateDataQuality_ruleset = f"""{dq_rules}"""

        EvaluateDataQualityMultiframe = EvaluateDataQuality().process_rows(
            frame=dyf,
            ruleset=EvaluateDataQuality_ruleset,
            publishing_options={
                "dataQualityEvaluationContext": "EvaluateDataQualityMultiframe",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True,
            },
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
        )

        rowLevelOutcomes = SelectFromCollection.apply(
            dfc=EvaluateDataQualityMultiframe,
            key="rowLevelOutcomes",
            transformation_ctx="rowLevelOutcomes",
        )

        rowLevelOutcomes_df = rowLevelOutcomes.toDF()

        column_count = len(rowLevelOutcomes_df.columns)

        rowLevelOutcomes_df = rowLevelOutcomes_df.withColumn(
            "DataQualityEvaluationResult",
            F.expr(f"""
                CASE
                    WHEN DataQualityEvaluationResult = 'Passed' AND 
                         {column_count} = {column_count_expected} AND 
                         LENGTH(TRIM({pk})) > 1 THEN 'Passed'
                    ELSE 'Failed'
                END
            """)
        )

        df_passed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Passed")
        df_failed = rowLevelOutcomes_df.filter(rowLevelOutcomes_df.DataQualityEvaluationResult == "Failed")

        self.logger.info(f"Counts of Rows Passed: {df_passed.count()}")
        self.logger.info(f"Counts of Rows Failed: {df_failed.count()}")

        if df_passed.isEmpty():
            if df_failed.isEmpty():
                raise Exception("Issue loading source data in raw bucket")
            else:
                raise Exception("Data Quality for the whole dataset has failed.")

        if not df_failed.isEmpty():
            self.error_handling_after_dq(df_failed,bucket_name,s3_path_prefix,output_file_type)

        return df_passed
        

    def error_handling_after_dq(self, df, bucket_name, s3_path_prefix, output_file_type):
        """
        Handles the records that have failed the data quality check. It exports the files
        to a specific S3 file path and changes the sns_trigger boolean to True so 
        at the end of the pipeline, it knows to send a notification to the end-user.

        This method can be overridden for each class, especially when dealing with CSVs.

        Args:
            df (DataFrame): The DataFrame containing the records.
            bucket_name (str): The name of the S3 bucket.
            s3_path_prefix (str): The prefix of the S3 file path.
            output_file_type (str): The type of the output file. It can be 'json' or 'csv'.

        Raises:
            Exception: An error occurred during the process.
        """

        self.logger.info('Exporting the records that have failed data quality checks...')
        self.dropping_dq_columns(df)
        workflow_run_id = self.aws_instance.get_glue_env_var('WORKFLOW_RUN_ID')
        error_path = f"s3://{bucket_name}/error/{s3_path_prefix}/{workflow_run_id}/dq_fail_rows/"

        self.logger.info(f'Exporting data to {error_path}')

        if output_file_type == 'json':

            df.coalesce(1).write.json(error_path,mode="overwrite", lineSep="\n",ignoreNullFields = False)

            # have to rename the error folder file so that it alligns with what enters the folders
            files_in_s3_path = self.aws_instance.get_files_in_s3_path(error_path)
            for i in files_in_s3_path:
                parts = i.split('/')
                parts[-1] = self.generate_random_filename()
                new_key = '/'.join(parts)
                self.aws_instance.move_s3_object(bucket_name, i, new_key)

        elif output_file_type == 'csv':
            df.coalesce(1).write.csv(error_path, mode="overwrite", header=True)

        else:
            raise Exception("No output filetype was specified.")
        
        # Boolean will now trigger sns at end of pipeline since there is an error detected
        self.sns_trigger = True

        return



    def generate_random_filename(self):
        """
        Generate a random filename in the format "fe9bc83d-36ac-3c98-be1e-392306368c19-yyyy-mm-dd-hhmmss
        to replicate an amazon-appflow id. Hence when reprocessed pipeline will not fail
        "

        Returns:
            str: A randomly generated filename.
        """
        characters = string.hexdigits.lower()
        random_string = ''.join(random.choice(characters) for _ in range(32))
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
        return "-".join([random_string[:8], random_string[8:12], random_string[12:16], random_string[16:20], random_string[20:], timestamp])



    @transformation_timer
    def redact_pii_columns(self,df, input_columns):
        """
        Apply redaction to specified columns in the DataFrame.
        Args:
            df (DataFrame): The input Spark DataFrame.
            input_columns (list): List of column names where PII redaction is to be applied.
        Returns:
            DataFrame: The DataFrame with redacted columns.
        """

        self.logger.info('Annonymising the PII information....')
        
        # Redaction UDF
        def _redact_text(text):
            phone_regex = r'(\+|\b07)([\d\s.-]*\d)'
            email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            regex_mappings = {
                phone_regex: 'XXXXXX',
                email_regex: 'YYYYYY'
            }
            
            if text is not None and isinstance(text, str):
                for pattern, replacement in regex_mappings.items():
                    text = re.sub(pattern, replacement, text)
            return text

        redact_udf = F.udf(_redact_text, StringType())

        # Redact PII in specified columns
        for column_name in input_columns:
            df = df.withColumn(column_name, redact_udf(F.col(column_name).cast(StringType())))

        return df
        

    @transformation_timer
    def transform_struct_to_string(self, df):
        """
        Transform struct fields in the DataFrame to JSON strings.
        
        Parameters:
        - df (DataFrame): Input DataFrame with possibly nested struct fields.
        
        Returns:
        - DataFrame: DataFrame with struct fields converted to JSON strings.
        """
        for field in df.schema.fields:
            # Check if the field is of StructType
            if isinstance(field.dataType, StructType):
                # Convert the struct field to a JSON String
                df = df.withColumn(field.name, F.to_json(F.col(field.name)))
        return df

    @transformation_timer
    def adding_cdc_columns(self, df):
        """
        Add change data capture (CDC) columns to the DataFrame.

        This function adds three columns to the DataFrame:
        - 'cdc_timestamp': Current timestamp indicating the time of data capture.
        - 'cdc_glue_workflow_id': Glue workflow ID retrieved from AWS environment variables.
        - 'unique_guid': Unique GUID generated for each row.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: DataFrame with CDC columns added.
        """

        # Add current timestamp column
        df = df.withColumn("cdc_timestamp", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))

        # Add glue workflow ID
        df = df.withColumn("cdc_glue_workflow_id", F.lit(self.aws_instance.get_glue_env_var('WORKFLOW_RUN_ID')))

        # Add unique GUID column
        df = df.withColumn("unique_guid", F.monotonically_increasing_id().cast("string"))

        return df

    def create_partition_date_columns(self,df, timestamp_column):
        """
        Extracts day, month, and year from a timestamp column and adds them as partition columns with string type.

        Args:
            df (DataFrame): Input PySpark DataFrame.
            timestamp_column (str): Name of the timestamp column containing dates in the format dd-mm-yyyy HH:mm:ss.

        Returns:
            DataFrame: PySpark DataFrame with day, month, and year extracted as partition columns.

        """
        # Split the timestamp into date and time components
        df = df.withColumn("date_components", F.split(F.col(timestamp_column), " "))
        
        # Split the date component into day, month, and year
        df = df.withColumn("day_partition", F.split(df.date_components[0], "-")[0].cast("string"))
        df = df.withColumn("month_partition", F.split(df.date_components[0], "-")[1].cast("string"))
        df = df.withColumn("year_partition", F.split(df.date_components[0], "-")[2].cast("string"))
        
        # Drop the intermediate column
        df = df.drop("date_components")
        
        return df

    def read_data_from_s3(self,bucket_name,file_path, file_format='json',appflow_config = None):
        """
        Read data from S3 based on the specified file format.

        Parameters:
            bucket_name (str): The name of the S3 bucket.
            file_path (str): The path to the file in the S3 bucket.
            file_format (str, optional): The format of the file to read. Supported formats: 'json', 'csv','delta'. Defaults to 'json'.
            appflow_config (str, optional): If there is an appflow config, it is passed in to get rows extracted. Defaults to None.

        Returns:
            DataFrame: The DataFrame containing the read data.
        """
        # Log the file path from where data is being read
        self.logger.info(f'Reading data in the file path: s3://{bucket_name}/{file_path}/')

        if file_format == 'json':
            source_df = self.spark.read.json(f"s3://{bucket_name}/{file_path}/")

        elif file_format == 'csv':
            source_df = self.spark.read.csv(f"s3://{bucket_name}/{file_path}/", header=True)

        elif file_format == 'delta':

            source_df = self.spark.read.format("delta").load(f"s3://{bucket_name}/{file_path}/")
            # Find the maximum date in the 'cdc_timestamp' column
            max_date = source_df.select(F.max("cdc_timestamp")).collect()[0][0]

            self.logger.info(f"Max date is {max_date}")

            # Filter DataFrame to select rows with the maximum date
            source_df = source_df.filter(F.col("cdc_timestamp") == max_date)

        else:
            raise ValueError("Unsupported file format. Supported formats: 'json', 'csv','delta'.")

        # Log the number of records in the DataFrame
        self.logger.info(f'Number of records in dataframe: {source_df.count()}')

        # Log the number of records processed from AppFlow if available
        if appflow_config is not None:
            # Extract the number of records processed from AppFlow
            appflow_row_number = self.aws_instance.extract_appflow_records_processed(self.list_of_files, appflow_config)
            self.logger.info(f'Number of records processed from appflow: {appflow_row_number}')

        return source_df
    

    @transformation_timer
    def dropping_duplicates(self, df):
        """
        Remove duplicate records from the DataFrame.

        Parameters:
            df (DataFrame): The DataFrame to remove duplicates from.

        Returns:
            DataFrame: The DataFrame with duplicates removed.
        """
        self.logger.info('Removing duplicate records')
        initial_count = df.count()
        df = df.dropDuplicates()
        new_count = df.count()
        self.logger.info(f"{initial_count - new_count} duplicate records have been removed")
        return df


    @transformation_timer
    def merge_to_delta_table(self, df, save_output_path, matching_columns):
        """
        Merge data from DataFrame to the Delta table using specified column matching criteria.

        Parameters:
            df (DataFrame): The DataFrame to be merged.
            save_output_path (str): The path to the Delta table to merge into.
            matching_columns (list): A list of column names for matching records.
        """
        # Create or replace temporary view for DataFrame
        df.createOrReplaceTempView("temp_view")

        # Construct matching conditions for the merge query
        conditions = " AND ".join([f"target.{col} = source.{col}" for col in matching_columns])

        # Construct the merge query
        sql_query = f"""
        MERGE INTO delta.`{save_output_path}` AS target
        USING temp_view AS source
        ON {conditions}
        WHEN MATCHED THEN
        UPDATE SET *
        WHEN NOT MATCHED THEN
        INSERT *
        """

        # Execute the merge query
        self.logger.info(f'Starting merge query: {sql_query}')
        self.spark.sql(sql_query)

        self.logger.info("Merge operation completed successfully.")

    @transformation_timer
    def remove_trailing_whitespace(self, df):
        """
        Remove trailing spaces from string columns

        Parameters:
            df (DataFrame): The PySpark DataFrame to clean.

        Returns:
            DataFrame: The cleaned DataFrame.
        """
        # Remove trailing spaces only from string columns
        for col_name, col_type in df.dtypes:
            if col_type == StringType():
                df = df.withColumn(col_name, F.rtrim(col_name))
        return df

    @transformation_timer
    def change_column_names_and_schema(self,df, column_mapping):
        """
        Change column names and schema of a PySpark DataFrame.

        Parameters:
            df (DataFrame): The PySpark DataFrame to modify.
            column_mapping (dict): A dictionary mapping original column names to new column names and schemas.
        
        Example:
            column_mapping = {
                'name'  : ('full_name','string'),
                'age'   : ('years_old','integer')  
            }

        Returns:
            DataFrame: The modified DataFrame with updated column names and schema.
        """
        self.logger.info('Changing column names and data type')
        # Rename columns and update data types
        for old_col_name, (new_col_name, new_col_type) in column_mapping.items():
            df = df.withColumnRenamed(old_col_name, new_col_name)
            df = df.withColumn(new_col_name, df[new_col_name].cast(new_col_type))

        return df

    def drop_columns_for_processed(self,df,*columns_to_drop):
        """
        Drop specified columns from a PySpark DataFrame.

        Parameters:
            df (DataFrame): The PySpark DataFrame.
            *columns_to_drop (str): Optional column names to drop.

        Returns:
            DataFrame: The DataFrame with specified columns dropped.
        """
        self.logger.info("Dropping the DQ columns")
        # List of fixed columns to be dropped
        fixed_columns_to_drop = [
            'dataqualityrulespass', 
            'dataqualityrulesfail', 
            'dataqualityrulesskip', 
            'dataqualityevaluationresult', 
            'cdc_timestamp', 
            'cdc_glue_workflow_id',
            'unique_guid'
        ]

        if columns_to_drop:
            # Concatenate the fixed columns with the optional columns
            all_columns_to_drop = fixed_columns_to_drop + list(columns_to_drop)
        else:
            # If no extra columns are passed, only drop fixed columns
            all_columns_to_drop = fixed_columns_to_drop

        # Drop specified columns
        modified_df = df.drop(*all_columns_to_drop)

        return modified_df