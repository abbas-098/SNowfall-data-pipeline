from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable

class ProcessedTradingHours(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "ods/trading_hours"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Removes trailing whitespaces
        2. Changes column names and schema
        3. Drops the data quality columns and CDC

        Parameters:
        - df: Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.

        """
        # Step 1: Removes trailing whitespaces
        df = self.remove_trailing_whitespace(df)

        column_mapping = {
            'store_number': ('store_number', 'integer'),
            'channel': ('channel', 'string'),
            'monday': ('monday_opening_times', 'string'),
            'tuesday': ('tuesday_opening_times', 'string'),
            'wednesday': ('wednesday_opening_times', 'string'),
            'thursday': ('thursday_opening_times', 'string'),
            'friday': ('friday_opening_times', 'string'),
            'saturday': ('saturday_opening_times', 'string'),
            'sunday': ('sunday_opening_times', 'string'),
            'cdc_timestamp':('cdc_timestamp','string')
            }
        
        # 2. Changes column names and schema
        df = self.change_column_names_and_schema(df,column_mapping)
        # 3. Drops the data quality columns and CDC
        df = self.drop_columns_for_processed(df)

        return df


    def save_data(self, df):
        """
        Save DataFrame to an S3 location and create/update a Delta table if needed.

        Parameters:
        - df (DataFrame): Input DataFrame to be saved.

        """
        # Define the S3 save path
        save_output_path = f"s3://{self.processed_bucket_name}/{self.file_path}/"

        # Check if Delta table needs to be created
        if DeltaTable.isDeltaTable(self.spark,save_output_path) is False:
            self.athena_trigger = True
            
        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:
            # Create the Delta table
            df.write.format("delta").mode("overwrite").save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('processed', 'ods_trading_hours', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['store_number','channel']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')



