from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger
from snowfall_pipeline.common_utilities.aws_utilities import AwsUtilities
from snowfall_pipeline.common_utilities.decorators import transformation_timer




from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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
        logger (Logger)                 : The logger for logging information.
        spark (SparkSession)            : The Spark session.
        sc (SparkContext)               : The Spark context.
        glueContext (GlueContext)       : The Glue context.
        sns_trigger (bool)              : At end of pipeline workflow determine if sns message needs to be sent if any error records detected.
        aws_instance (class instance)   : Initialise the AwsUtilities class so that the methods are available to use instead of having to import for each class
    """

    def __init__(self,spark,sc,glueContext):

        self.logger = SnowfallLogger.get_logger()
        self.spark = spark
        self.sc = sc
        self.glueContext = glueContext
        self.sns_trigger = False # TODO Should this be a default False param or not needed at all?
        self.aws_instance = AwsUtilities()



    def get_data(self):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def transform_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def save_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass


    def pipeline_flow(self):
        "Abstract method which runs each pipeline in order"
        df = self.get_data()
        tansformed_df = self.transform_data(df)
        self.save_data(tansformed_df)


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
    def data_quality_check(self, df, dq_rules):
         """Perform data quality checks on the DataFrame and returns the passed rows.

         Args:
             df (DataFrame): The input DataFrame to perform data quality checks on.
             dq_rules (string): The string from config files regarding the dq rules

         Returns:
             DataFrame: The DataFrame containing rows that passed the data quality checks. 
             ( Note that 4 extra columns are added due to this data quality check )

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
             F.expr(f"""CASE WHEN DataQualityEvaluationResult = 'Passed' AND {column_count} = {column_count_expected} THEN DataQualityEvaluationResult ELSE 'Failed' END""")
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
             self.error_handling_after_dq(df_failed)

         return df_passed
        

    def error_handling_after_dq(self,df,bucket_name,s3_path_prefix,output_file_type):
        """
        Handles the records that have failed the data quality check. It exports the files
        to a specific S3 file path and changes the sns_trigger boolean to True so 
        at the end of the pipeline, it knows to send a notification to the end-user.

        This method can be overridden for each class, especially when dealing with CSVs.

        Args:
            df (DataFrame): The DataFrame containing the records.
            bucket_name (str): The name of the S3 bucket.
            s3_path_prefix (str): The prefix of the S3 file path.
            output_file_type (str): The type of the output file as it can be json or csv.

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
        


    def dropping_dq_columns(self, df):

        """Drop Data Quality related columns from the input DataFrame.

        Args:
            df (DataFrame): The input DataFrame containing Data Quality columns.

        Returns:
            DataFrame: The DataFrame with Data Quality columns dropped.
        """
        columns_to_drop = ["DataQualityRulesPass", "DataQualityRulesFail", "DataQualityRulesSkip", "DataQualityEvaluationResult"]
        return df.drop(*columns_to_drop)

