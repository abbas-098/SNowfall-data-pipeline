from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from snowfall_pipeline.common_utilities.decorators import transformation_timer

class ProcessedSysUser(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/sys_user"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Splits JSON column
        2. Splits datetime column
        3. Join with Location table
        4. Filters passed records
        5. Gets unique records
        6. Drops unnecessary columns
        7. Selecting columns to take to processed layer
        8. Change column names and schema.

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.
        """

        # Step 1: Splits JSON column
        df = self.split_json_column(df, self.pipeline_config.get('transform_json'))

        # Step 2: Splits datetime column
        df = self.split_datetime_column(df, self.pipeline_config.get('process_timestamp'))

        # Step 3: Joining with location table
        df = self.join_location_table(df,'location_display_value')

        # Step 4: Filters passed records
        df = self.filter_quality_result(df)

        # Step 5: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 6: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)


        # Step 7: Selecting Columns that I want to take to processed layer
        df = df.select(
            'active',
            'avatar',
            'calendar_integration',
            'city',
            'country',
            'date_format',
            'email',
            'employee_number',
            'enable_multifactor_authn',
            'failed_attempts',
            'first_name',
            'gender',
            'home_phone',
            'internal_integration_user',
            'introduction',
            'last_login',
            'last_name',
            'manager',
            'middle_name',
            'mobile_phone',
            'name',
            'notification',
            'phone',
            'photo',
            'preferred_language',
            'schedule',
            'source',
            'sso_source',
            'state',
            'street',
            'sys_class_name',
            'sys_created_by',
            'sys_domain_path',
            'sys_id',
            'sys_mod_count',
            'sys_tags',
            'sys_updated_by',
            'time_format',
            'time_zone',
            'title',
            'u_delivery_kitchen',
            'u_grade',
            'u_mytech_va_store',
            'u_office',
            'u_otp',
            'u_store_no',
            'u_techseed',
            'u_user_account_control',
            'u_user_type',
            'user_name',
            'vip',
            'web_service_access_only',
            'zip',
            'created_year',
            'created_month',
            'building_display_value',
            'building_link',
            'sys_domain_display_value',
            'sys_domain_link',
            'cost_center_display_value',
            'cost_center_link',
            'company_display_value',
            'company_link',
            'department_display_value',
            'department_link',
            'location_display_value',
            'location_link',
            'last_login_time_dt',
            'last_login_time_timestamp',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'sys_created_on_dt',
            'sys_created_on_timestamp',
            'restaurant_full_name',
            'restaurant_name',
            'restaurant_id'
        )

        column_mapping = {
            'user_name': ('user_name', 'string'),
            'employee_number': ('employee_number', 'string'),
            'sys_id': ('sys_id', 'string'),
            'last_login_time_dt': ('last_login_time_date', 'date'),
            'last_login_time_timestamp': ('last_login_timestamp', 'string'),
            'sys_updated_on_dt': ('sys_updated_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
            'sys_created_on_dt': ('sys_created_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_timestamp', 'string'),
            'restaurant_full_name': ('restaurant_full_name', 'string'),
            'restaurant_name': ('restaurant_name', 'string'),
            'restaurant_id': ('restaurant_id', 'integer'),
            'active': ('active_flag', 'boolean'),
            'avatar': ('avatar', 'string'),
            'calendar_integration': ('calendar_integration', 'string'),
            'city': ('city', 'string'),
            'country': ('country', 'string'),
            'date_format': ('date_format', 'string'),
            'email': ('email', 'string'),
            'enable_multifactor_authn': ('enable_multifactor_authn', 'boolean'),
            'failed_attempts': ('failed_attempts', 'integer'),
            'first_name': ('first_name', 'string'),
            'gender': ('gender', 'string'),
            'home_phone': ('home_phone', 'string'),
            'internal_integration_user': ('internal_integration_user_flag', 'boolean'),
            'introduction': ('introduction', 'string'),
            'last_login': ('last_login', 'string'),
            'last_name': ('last_name', 'string'),
            'manager': ('manager', 'string'),
            'middle_name': ('middle_name', 'string'),
            'mobile_phone': ('mobile_phone', 'string'),
            'name': ('name', 'string'),
            'notification': ('notification', 'string'),
            'phone': ('phone', 'string'),
            'photo': ('photo', 'string'),
            'preferred_language': ('preferred_language', 'string'),
            'schedule': ('schedule', 'string'),
            'source': ('source', 'string'),
            'sso_source': ('sso_source', 'string'),
            'state': ('state', 'string'),
            'street': ('street', 'string'),
            'sys_class_name': ('sys_class_name', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_domain_path': ('sys_domain_path', 'string'),
            'sys_mod_count': ('sys_mod_count', 'integer'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'time_format': ('time_format', 'string'),
            'time_zone': ('time_zone', 'string'),
            'title': ('title', 'string'),
            'u_delivery_kitchen': ('u_delivery_kitchen', 'string'),
            'u_grade': ('u_grade', 'string'),
            'u_mytech_va_store': ('u_mytech_va_store', 'string'),
            'u_office': ('u_office', 'string'),
            'u_otp': ('u_otp', 'string'),
            'u_store_no': ('store_number', 'integer'),
            'u_techseed': ('u_techseed', 'string'),
            'u_user_account_control': ('u_user_account_control', 'string'),
            'u_user_type': ('u_user_type', 'string'),
            'vip': ('vip', 'boolean'),
            'web_service_access_only': ('web_service_access_only_flag', 'boolean'),
            'zip': ('zip', 'string'),
            'created_year': ('created_year', 'integer'),
            'created_month': ('created_month', 'integer'),
            'building_display_value': ('building', 'string'),
            'building_link': ('building_cmn_link', 'string'),
            'sys_domain_display_value': ('sys_domain', 'string'),
            'sys_domain_link': ('sys_domain_sys_user_group_link', 'string'),
            'cost_center_display_value': ('cost_center', 'string'),
            'cost_center_link': ('cost_center_link', 'string'),
            'company_display_value': ('company', 'string'),
            'company_link': ('company_link', 'string'),
            'department_display_value': ('department', 'string'),
            'department_link': ('department_link', 'string'),
            'location_display_value': ('location', 'string'),
            'location_link': ('location_cmn_link', 'string')
        }

        # 8. Changes column names and schema
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
            self.aws_instance.create_athena_delta_table('processed', 'service_now_sys_user', save_output_path, self.athena_output_path)
            
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