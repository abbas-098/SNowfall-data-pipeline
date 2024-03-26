from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.data_quality_rules import dq_rules
from delta.tables import DeltaTable


# dataset incident_daily

class PreparationIncidentDaily(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs['incidents'] # have to hard code incidents here since have daily and intra
        self.dq_rule = dq_rules.get('incidents')
        self.file_path = "service_now/incident/daily"
        self.list_of_files = self.aws_instance.get_files_in_s3_path(f"{self.raw_bucket_name}/{self.file_path}/")


    def get_data(self):
        df = self.read_data_from_s3(self.raw_bucket_name,self.file_path,'json',self.pipeline_config.get('appflow_name_daily'))
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Remove duplicate records.
        2. Convert all structs to strings.
        3. Remove trailing whitespaces
        4. Perform data quality check.
        5. Mask PII Data
        6. Add CDC columns.
        7. Add Partition Columns

        Parameters:
        - df: Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.

        """

        # Step 1: Remove duplicate records
        df = self.dropping_duplicates(df)

        # Step 2: Convert all structs to strings
        df = self.transform_struct_to_string(df)

        # Step 3: Removes trailing whitespaces
        df = self.remove_trailing_whitespace(df)

        # Step 4: Data quality check
        df = self.data_quality_check(df, self.dq_rule,self.pipeline_config.get('primary_key'), self.raw_bucket_name, self.file_path, 'json')

        # Step 5: Mask PII Information
        df = self.redact_pii_columns(df,self.pipeline_config.get('redact_pii_columns'))

        # Step 6: Add CDC columns
        df = self.adding_cdc_columns(df)

        # Step 7: Adding Partiton Columns
        df = self.create_partition_date_columns(df,'sys_created_on','created')

        return df



    def save_data(self, df):
        """
        Save DataFrame to an S3 location and create/update a Delta table if needed.

        Parameters:
        - df (DataFrame): Input DataFrame to be saved.

        """
        # Define the S3 save path
        save_output_path = f"s3://{self.preparation_bucket_name}/{self.file_path}/"

        # Check if Delta table needs to be created
        if DeltaTable.isDeltaTable(self.spark,save_output_path) is False:
            self.athena_trigger = True
            
        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:

            # Create the Delta table
            df.write.format("delta").mode("overwrite") \
            .partitionBy('created_year','created_month') \
            .save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('preparation', 'service_now_incident_daily', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['number','sys_created_on','state','created_year','created_month']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        # Move files to the Archive folder
        for file_name in self.list_of_files:
            self.aws_instance.move_s3_object(self.raw_bucket_name, file_name, f"archive/{file_name}")
        
        # If error detected from DQ failing then will raise
        if self.sns_trigger:
            message = "Records in the error folder that have failed DQ rules"
            self.aws_instance.send_sns_message(message)
        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')
