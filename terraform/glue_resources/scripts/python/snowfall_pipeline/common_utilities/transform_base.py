from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger
from snowfall_pipeline.common_utilities.aws_utilities import AwsUtilities
from snowfall_pipeline.common_utilities.decorators import transformation_timer


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
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
        file_path : Currently set at None, but when each class inherits this it will set a value. Set here so I can use logic to filter records.
        account_number (str): Account number that we utilize in AWS used to generate the bucket names.
        environment (str): Environment that we utilize in AWS used to generate the bucket names.
        full_config (dict): Full configuration read from the script_config file in the common_utilities folders.
        raw_bucket_name (str): Name of the raw bucket.
        preparation_bucket_name (str): Name of the preparation bucket.
        processed_bucket_name (str): Name of the processed bucket.
        semantic_bucket_name (str): Name of the semantic bucket.
        datasets (str) : Dataset name pulled from glue workflow propeties
        group (str) : Group name pulled from glue workflow propeties
    """

    def __init__(self,spark,sc,glueContext):
        self.logger = SnowfallLogger.get_logger()
        self.spark = spark
        self.sc = sc
        self.glueContext = glueContext
        self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
        self._initial_message_printed()
        self.aws_instance = AwsUtilities()
        self.sns_trigger = False
        self.athena_trigger = False
        self.list_of_files = None
        self.file_path = None
        self.account_number = self.aws_instance.get_glue_env_var('ACCOUNT_NUMBER')
        self.environment = self.aws_instance.get_glue_env_var('ENVIRONMENT')
        self.full_configs = self.aws_instance.reading_json_from_zip()
        self.raw_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-raw-{self.account_number}"
        self.preparation_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-preparation-{self.account_number}"
        self.processed_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-processed-{self.account_number}"
        self.semantic_bucket_name = f"eu-central1-{self.environment}-uk-snowfall-semantic-{self.account_number}"
        self.athena_output_path = f"eu-central1-{self.environment}-uk-snowfall-athena-{self.account_number}/"
        self.datasets = self.aws_instance.get_workflow_properties('DATASET')
        self.group = self.aws_instance.get_workflow_properties('GROUP')



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
        pattern = r'ColumnCount\s*<=\s*(\d+)'
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
                         {column_count} <= {column_count_expected} AND 
                         LENGTH(TRIM({pk})) >= 1 THEN 'Passed'
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
        

    def error_handling_after_dq(self, df, bucket_name, s3_path_prefix, output_file_type,partition_column_drop = None):
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
            partition_column_drop (list) : List of extra columns needed to be dropped if processed pipeline is being triggered

        Raises:
            Exception: An error occurred during the process.
        """

        self.logger.info('Exporting the records that have failed data quality checks...')

        workflow_run_id = self.aws_instance.get_glue_env_var('WORKFLOW_RUN_ID')
        error_path = f"s3://{bucket_name}/error/{s3_path_prefix}/{workflow_run_id}/dq_fail_rows/"
        
        columns_to_drop = ['dataqualityrulespass','dataqualityrulesfail','dataqualityrulesskip','dataqualityevaluationresult']

        if not 'Preparation' in self.__class__.__name__:
            columns_to_drop = columns_to_drop + ['cdc_timestamp','cdc_glue_workflow_id','unique_guid']
            error_path = f"s3://{bucket_name}/error/{s3_path_prefix}/{workflow_run_id}/transformation_fail/"

        if partition_column_drop:
            columns_to_drop = columns_to_drop + partition_column_drop

        df = df.drop(*columns_to_drop)

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

    def create_partition_date_columns(self,df, timestamp_column, prefix):
        """
        Extracts month and year from a timestamp column and adds them as partition columns with string type.

        Args:
            df (DataFrame): Input PySpark DataFrame.
            timestamp_column (str): Name of the timestamp column containing dates in the format dd-mm-yyyy HH:mm:ss or yyyy-MM-dd HH:mm:ss.
            prefix (str) : Prefix you want with the column name

        Returns:
            DataFrame: PySpark DataFrame with month and year extracted as partition columns.

        """
        # Get the first value of the timestamp column
        first_timestamp_value = df.select(timestamp_column).head()[timestamp_column]

        # Check the length of the date components after splitting by "-"
        if len(first_timestamp_value.split("-")[0]) == 4:
            # If the first component has length 4, assume the format is 'yyyy-MM-dd HH:mm:ss'
            timestamp_format = "yyyy-MM-dd HH:mm:ss"
        else:
            # Otherwise, assume the format is 'dd-MM-yyyy HH:mm:ss'
            timestamp_format = "dd-MM-yyyy HH:mm:ss"

        # Split the timestamp into date and time components
        df = df.withColumn("date_components", F.split(F.col(timestamp_column), " "))

        # Extract year and month from the timestamp column based on the detected format
        if timestamp_format == "dd-MM-yyyy HH:mm:ss":
            df = df.withColumn(f"{prefix}_year", F.split(F.col("date_components")[0], "-")[2].cast("integer"))
            df = df.withColumn(f"{prefix}_month", F.split(F.col("date_components")[0], "-")[1].cast("integer"))
        else:
            df = df.withColumn(f"{prefix}_year", F.year(F.col("date_components")[0]).cast("integer"))
            df = df.withColumn(f"{prefix}_month", F.month(F.col("date_components")[0]).cast("integer"))

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

        self.logger.info(f"Files processed: {self.list_of_files}")

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
                df = df.withColumn(col_name, F.trim(col_name))
        return df

    @transformation_timer
    def change_column_names_and_schema(self, df, column_mapping):
        """
        Change column names and schema of a PySpark DataFrame.

        Parameters:
            df (DataFrame): The PySpark DataFrame to modify.
            column_mapping (dict): A dictionary mapping original column names to new column names and schemas.

        Example:
            column_mapping = {
                'name': ('full_name', 'string'),
                'age': ('years_old', 'integer'),
                'time_column': ('time_column', 'time')  # Add time column here
            }

        Returns:
            DataFrame: The modified DataFrame with updated column names and schema.
        """
        self.logger.info('Changing column names and data type')
        # Rename columns and update data types
        # Construct a list of column expressions for renaming and type casting
        expressions = [
            F.col(old_col_name).alias(new_col_name).cast(new_col_type)
            if new_col_type != 'time'
            else F.date_format(F.col(old_col_name), 'HH:mm:ss').alias(new_col_name)
            for old_col_name, (new_col_name, new_col_type) in column_mapping.items()
        ]

        # Apply the expressions to the DataFrame
        df = df.select(*expressions)

        return df


    @transformation_timer
    def split_json_column(self,df, columns):
        """
        Parse JSON strings in specified columns of a DataFrame and create new columns.
        
        Args:
        - df: DataFrame to operate on.
        - columns: List of column names containing JSON strings.
        
        Returns:
        - DataFrame with new columns containing parsed JSON data.
        """
        self.logger.info('Running the split_json_column function')
        for column in columns:

            # Define schema for the JSON
            json_schema = "display_value string, link string"
            
            # Apply from_json function to parse JSON string
            df = df.withColumn(f"{column}_json_data", F.from_json(F.col(column), json_schema))
            
            # Extract display value and link
            df = df.withColumn(f"{column}_display_value", F.col(f"{column}_json_data.display_value"))
            df = df.withColumn(f"{column}_link", F.regexp_extract(F.col(f"{column}_json_data.link"), r'([^/]+)$', 1))
            
            # Drop intermediate JSON column
            df = df.drop(f"{column}_json_data")
        
        return df
    
    
    @transformation_timer
    def split_datetime_column(self, df, input_columns):
        """
        Transform the DataFrame by converting timestamp columns, splitting them, and updating data quality metrics.

        Args:
            df (DataFrame): The input Spark DataFrame.
            input_columns (list): List of column names to be processed for timestamp conversion.

        Returns:
            DataFrame: The processed Spark DataFrame.
        """
        self.logger.info('Running the split_datetime_column function')
        timestamp_formats = ["yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss"]
        
        for col_name in input_columns:
            check = F.lit(None).cast("timestamp")
            for fmt in timestamp_formats:
                current_check = F.unix_timestamp(F.col(col_name), fmt).cast("timestamp")
                check = F.coalesce(check, current_check)

            df = df.withColumn(f"{col_name}_checked", check)

            df = df.withColumn(
                f"{col_name}_dt", F.col(f"{col_name}_checked").cast("date")
            ).withColumn(
                f"{col_name}_timestamp", F.date_format(F.col(f"{col_name}_checked"), "yyyy-MM-dd HH:mm:ss")
            )

            df = df.withColumn(
                "DataQualityEvaluationResult",
                F.when(
                    (F.col(col_name).isNull() | (F.col(col_name) == "") | F.col(f"{col_name}_checked").isNull()),
                    F.lit("Failed")
                ).otherwise(F.col("DataQualityEvaluationResult"))
            )

            df = df.drop(f"{col_name}_checked")

        return df


    @transformation_timer
    def filter_quality_result(self, df, partition_column_drop = None):
        """
        Filter DataFrame records based on the DataQualityEvaluationResult.

        Args:
            df (DataFrame): The input Spark DataFrame.
            partition_column_drop (list) : List of extra partition columns that need dropping before exporting

        Returns:
            DataFrame: The filtered DataFrame containing only passed records.
        """

        self.logger.info('Running the filter_quality_result function')

        df_failed = df.filter(df["DataQualityEvaluationResult"] == "Failed")

        if not df_failed.isEmpty():
            self.logger.info('Handling failed records...')
            # Reading data from preparation bucket
            prep_df = self.spark.read.format("delta").load(f"s3://{self.preparation_bucket_name}/{self.file_path}/")
            error_records_df = prep_df.join(df_failed.select("cdc_timestamp", "unique_guid"), on=["cdc_timestamp", "unique_guid"], how="inner")
            self.error_handling_after_dq(error_records_df,self.preparation_bucket_name,self.file_path,'json',partition_column_drop)


        # Filter DataFrame for passed records and return it
        df_passed = df.filter(df["DataQualityEvaluationResult"] == "Passed")
        self.logger.info('Returning the DataFrame with passed records')
        return df_passed


    @transformation_timer
    def adding_seconds_column(self,df,input_columns):
        """
        Transform the input DataFrame by adding columns for each input column that represent the time in seconds.
        """

        @F.pandas_udf(LongType())
        def convert_time_to_seconds_udf(time_series):
            """
            Pandas UDF to convert time text to seconds.
            """
            unit_to_seconds = {
                "second": 1, "seconds": 1, "minute": 60, "minutes": 60,
                "hour": 3600, "hours": 3600, "day": 86400, "days": 86400,
                "week": 604800, "weeks": 604800, "year": 31536000, "years": 31536000,
            }

            def convert_time_to_seconds(time_text):
                try:
                    matches = re.findall(r"(\d+)\s*(\w+)", time_text if time_text else "")
                    return sum(
                        int(value) * unit_to_seconds[unit.lower()]
                        for value, unit in matches
                        if unit.lower() in unit_to_seconds
                    )
                except Exception:
                    return 0

            return time_series.apply(convert_time_to_seconds)

        for time_column in input_columns:
            df = df.withColumn(
                f"{time_column}_seconds", convert_time_to_seconds_udf(F.col(time_column))
            )
        return df


    @transformation_timer
    def join_location_table(self,df,joining_key):
        """
        Joins a spark dataframe with the location table

        Args:
            df (DataFrame): The main DataFrame to join.
            joining_key (str): The key column in the main DataFrame.

        Returns:
            DataFrame: The joined DataFrame with updated Data Quality columns.
        """
        self.logger.info('Running the join_location_table function')

        # Reading location table and specific key
        location_path = f"s3://{self.processed_bucket_name}/service_now/location/"
        if DeltaTable.isDeltaTable(self.spark,location_path) is False:
            raise Exception('Unable to join with location table since there is no data. Please run the location workflow.')
        
        location_df = self.spark.read.format("delta").load(location_path)
        location_df = location_df.select('restaurant_full_name','restaurant_name','restaurant_id')
        location_key = 'restaurant_full_name'

        # Perform the join
        joined_df = df.join(location_df, F.col(joining_key) == F.col(location_key), how="left")

        # Update Data Quality columns
        joined_df = joined_df.withColumn(
            "DataQualityEvaluationResult",
            F.when(
                F.col(location_key).isNull(),
                F.lit("Failed")
            ).otherwise(F.lit("Passed"))
        )

        return joined_df
    
    
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

    @transformation_timer
    def remove_commas_in_integer(self,df, column_list):
        """
        Remove commas from specified columns in a PySpark DataFrame.

        Args:
            df (DataFrame): The input PySpark DataFrame.
            column_list (list): A list of column names in the DataFrame from which commas should be removed.

        Returns:
            DataFrame: The DataFrame with commas removed from specified columns.
        """
        self.logger.info('Running the remove_commas_in_integer function..')
        for column_name in column_list:
            df = df.withColumn(column_name, F.regexp_replace(F.col(column_name), ',', ''))
        return df


    @transformation_timer
    def get_unique_records_sql(self, df,sql_query=None):
        """
        Run the SQL query on the dataframe.
        """
        self.logger.info('Running the get_unique_records function.')

        initial_count = df.count()
        if sql_query is None:

            sql_query = """
                SELECT a.*
                FROM my_dataframe a
                INNER JOIN (
                    SELECT number, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY number
                ) b ON a.number = b.number AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        df.createOrReplaceTempView("my_dataframe")
        unique_df = self.spark.sql(sql_query)
        final_count = unique_df.count()
        self.logger.info(f"{initial_count - final_count} rows have been removed after running get_unique_records function")
        return unique_df