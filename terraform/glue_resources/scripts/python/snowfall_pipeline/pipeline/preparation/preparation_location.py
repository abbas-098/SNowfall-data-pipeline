from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import *


class PreparationLocation(TransformBase):
    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        # when showing test change to 75
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

        """

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

        return df


    @transformation_timer
    def save_data(self, df):
        """
        Save DataFrame to an S3 location and create/update a Delta table if needed.

        Parameters:
        - df (DataFrame): Input DataFrame to be saved.

        """
        # Define the S3 save path
        save_output_path = f"s3://{self.preparation_bucket_name}/service_now/location/"

        # Check if Delta table needs to be created
        if not self.aws_instance.check_if_delta_table_exists(self.spark,save_output_path):
            self.athena_trigger = True
            
        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:
            # Create the Delta table
            df.write.format("delta").mode("overwrite").save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('preparation', 'service_now_location', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            df.createOrReplaceTempView("temp_view")
            sql_query = f"""
            MERGE INTO delta.`{save_output_path}` AS target
            USING temp_view AS source
            ON target.full_name = source.full_name
            AND target.sys_created_on = source.sys_created_on
            AND target.sys_id = source.sys_id
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT *
            """
            self.logger.info(f'Starting merge query: {sql_query}')
            self.spark.sql(sql_query)

        
        # Move files to the Archive folder
        for file_name in self.list_of_files:
            self.aws_instance.move_s3_object(self.raw_bucket_name, file_name, f"archive/{file_name}")
        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')



