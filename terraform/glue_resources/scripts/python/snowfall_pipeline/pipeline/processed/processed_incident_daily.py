from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import DeltaTable


class ProcessedIncidentDaily(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs['incidents']
        self.file_path = "service_now/incident/daily"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Splits JSON column
        2. Adding seconds column
        3. Splits datetime column
        4. Joining with location table
        5. Filters passed records
        6. Gets unique records
        7. Drops unnecessary columns
        8. Selecting columns to take to processed layer
        9. Change column names and schema.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.
        """

        # Step 1: Splits JSON column
        df = self.split_json_column(df, self.pipeline_config.get('transform_json'))

        # Step 2: Add seconds columns
        df = self.adding_seconds_column(df,self.pipeline_config.get('add_seconds'))

        # Step 3: Splits datetime column
        df = self.split_datetime_column(df,self.pipeline_config.get('process_timestamp'))

        # Step 4: Joining with location table
        df = self.join_location_table(df,'location_display_value')

        # Step 5: Filters passed records
        df = self.filter_quality_result(df)

        # Step 6: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 7: Drops unnecessary columns
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
                df.write.format("delta").mode("overwrite") \
                .partitionBy('created_year','created_month') \
                .save(save_output_path)

                # Execute Athena query to create the table
                self.aws_instance.create_athena_delta_table('processed', 'service_now_incident_daily', save_output_path, self.athena_output_path)
                
            else:

                # Merge data to the Delta table
                merge_columns = ['number','sys_created_on','state','created_year','created_month']
                self.merge_to_delta_table(df,save_output_path,merge_columns)

            
            self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')


    @transformation_timer
    def get_unique_records_sql(self,df):
        """
        Run the SQL query on the dataframe.
        """
        self.logger.info('Running the get_unique_records function.')
        df.createOrReplaceTempView("my_dataframe")
        query =  """
                SELECT a.*
                FROM my_dataframe a
                INNER JOIN (
                    SELECT number,state, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY number,state
                ) b ON a.number = b.number AND 
                       a.state = b.state AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)