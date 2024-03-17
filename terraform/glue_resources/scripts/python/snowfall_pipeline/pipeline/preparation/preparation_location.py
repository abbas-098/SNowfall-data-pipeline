from snowfall_pipeline.common_utilities.transform_base import TransformBase


class PreparationLocation(TransformBase):
    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.dq_rule = """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "sys_created_on"]"""
        self.file_path = "service_now/location"
        self.list_of_files = self.aws_instance.get_files_in_s3_path(f"{self.raw_bucket_name}/{self.file_path}/")


    def get_data(self):
        """
        Retrieve data from the specified file path.

        Reads JSON data from the specified S3 file path and logs the number of records in the DataFrame.
        If available, also logs the number of records processed from AppFlow.

        Returns:
        - DataFrame: DataFrame containing the retrieved data.
        """
        # Log the file path from where data is being read
        self.logger.info(f'Reading data in the file path: s3://{self.raw_bucket_name}/{self.file_path}/')
        
        # Read data from the specified S3 file path
        if len(self.list_of_files) > 0 :
            source_df = self.spark.read.json(f"s3://{self.raw_bucket_name}/{self.file_path}/")
        else:
            message = f"The file path: s3://{self.raw_bucket_name}/{self.file_path}/ is empty."
            # Since file is empty, just need to send sns.
            self.aws_instance.send_sns_message(message)
            raise Exception(message)
        
        # Extract the number of records processed from AppFlow
        appflow_row_number = self.aws_instance.extract_appflow_records_processed(self.list_of_files, self.pipeline_config['appflow_name'])
        
        # Log the number of records in the DataFrame
        self.logger.info(f'Number of records in dataframe: {source_df.count()}')
        
        # Log the number of records processed from AppFlow if available
        if appflow_row_number is not None:
            self.logger.info(f'Number of records processed from appflow: {appflow_row_number}')
        
        return source_df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Remove duplicate records.
        2. Perform data quality check.
        3. Convert all structs to strings.
        4. Add CDC columns.

        Parameters:
        - df: Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.

        Raises:
        - Exception: If an error occurs during the transformation process. In case of an error,
                    files from the list_of_files are moved to the 'error' folder in the S3 bucket,
                    and an SNS message is sent with the error details.
        """
        try:
            # Step 1: Remove duplicate records
            self.logger.info('Removing duplicate records')
            df = df.dropDuplicates()
            self.logger.info(f'Number of records in dataframe after dropping duplicates: {df.count()}')

            # Step 2: Data quality check
            df = self.data_quality_check(df, self.dq_rule, self.raw_bucket_name, self.file_path, 'json')

            # Step 3: Convert all structs to strings
            df = self.transform_struct_to_string(df)

            # Step 4: Add CDC columns
            df = self.adding_cdc_columns(df)

        except Exception as e:
            for i in self.list_of_files:
                self.aws_instance.move_s3_object(self.raw_bucket_name, i, f"error/{i}")  
            self.aws_instance.send_sns_message(e)  
            raise e 

        return df



    def save_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass

