from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.data_quality_rules import dq_rules
from delta.tables import DeltaTable


class PreparationLocationHierarchy(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.dq_rule = dq_rules.get(self.datasets)
        self.file_path = "ods/location_hierarchy"
        self.list_of_files = self.aws_instance.get_files_in_s3_path(f"{self.raw_bucket_name}/{self.file_path}/")


    def get_data(self):
        df = self.read_data_from_s3(self.raw_bucket_name,self.file_path,'csv')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Remove duplicate records.
        2. Perform data quality check.
        3. Add CDC columns.

        Parameters:
        - df: Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.

        """

        # Step 1: Remove duplicate records
        df = self.dropping_duplicates(df)

        # Step 2: Data quality check
        df = self.data_quality_check(df, self.dq_rule,self.pipeline_config.get('primary_key'), self.raw_bucket_name, self.file_path, 'json')

        # Step 3: Add CDC columns
        df = self.adding_cdc_columns(df)

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
        if DeltaTable.isDeltaTable(self.spark,save_output_path):
            self.athena_trigger = True
            
        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:
            # Create the Delta table
            df.write.format("delta").mode("overwrite").save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('preparation', 'ods_location_hierarchy', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['STORE_NUMBER','STORE_NAME']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        # Move files to the Archive folder
        for file_name in self.list_of_files:
            self.aws_instance.move_s3_object(self.raw_bucket_name, file_name, f"archive/{file_name}")
        
        # If error detected from DQ failing then will raise
        if self.sns_trigger:
            message = "Records in the error folder that have failed DQ rules"
            self.aws_instance.send_sns_message(message)
        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')



