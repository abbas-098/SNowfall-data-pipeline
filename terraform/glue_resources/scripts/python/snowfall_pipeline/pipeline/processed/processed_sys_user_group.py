from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from snowfall_pipeline.common_utilities.decorators import transformation_timer

class ProcessedSysUserGroup(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/sys_user_group"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Splits JSON column
        2. Splits datetime column
        3. Filters passed records
        4. Gets unique records
        5. Drops unnecessary columns
        6. Selecting columns to take to processed layer
        7. Change column names and schema.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.
        """

        # Step 1: Splits JSON column
        df = self.split_json_column(df, self.pipeline_config.get('transform_json'))

        # Step 2: Splits datetime column
        df = self.split_datetime_column(df, self.pipeline_config.get('process_timestamp'))

        # Step 3: Filters passed records
        df = self.filter_quality_result(df)

        # Step 4: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 5: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        # Step 6: Selecting Columns that I want to take to processed layer
        df = df.select(
            'active',
            'cost_center',
            'default_assignee',
            'description',
            'email',
            'exclude_manager',
            'include_members',
            'name',
            'parent',
            'source',
            'sys_created_by',
            'sys_id',
            'sys_mod_count',
            'sys_tags',
            'sys_updated_by',
            'type',
            'created_year',
            'created_month',
            'manager_display_value',
            'manager_link',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'sys_created_on_dt',
            'sys_created_on_timestamp'
        )

        column_mapping = {
            'sys_id': ('sys_id', 'string'),
            'manager_display_value': ('manager', 'string'),
            'sys_created_on_dt': ('sys_created_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_timestamp', 'string'),
            'sys_updated_on_dt': ('sys_updated_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
            'sys_mod_count': ('sys_mod_count', 'integer'),
            'active': ('active_flah', 'boolean'),
            'cost_center': ('cost_center', 'string'),
            'default_assignee': ('default_assignee', 'string'),
            'description': ('description', 'string'),
            'email': ('email', 'string'),
            'exclude_manager': ('exclude_manager', 'boolean'),
            'include_members': ('include_members', 'boolean'),
            'name': ('name', 'string'),
            'parent': ('parent', 'string'),
            'source': ('source', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'type': ('type', 'string'),
            'created_year': ('created_year', 'integer'),
            'created_month': ('created_month', 'integer'),
            'manager_link': ('manager_sys_user_link', 'string')
        }


        # 7. Changes column names and schema
        df = self.change_column_names_and_schema(df,column_mapping)

 

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
            self.aws_instance.create_athena_delta_table('processed', 'service_now_sys_user_group', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['sys_id','sys_created_on','created_month','created_year']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        # If error detected from DQ failing then will raise
        if self.sns_trigger:
            message = "Records in the error folder that have failed transformation"
            self.aws_instance.send_sns_message(message)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')




    @transformation_timer
    def get_unique_records_sql(self, df):
        """
        Run the SQL query on the dataframe.
        """
        self.logger.info('Running the get_unique_records function.')
        df.createOrReplaceTempView("my_dataframe")
        query =  """
                SELECT a.*
                FROM my_dataframe a
                INNER JOIN (
                    SELECT sys_id, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY sys_id
                ) b ON a.sys_id = b.sys_id AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)