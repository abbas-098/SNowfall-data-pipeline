from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from snowfall_pipeline.common_utilities.decorators import transformation_timer

class ProcessedLocation(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/location"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Splits JSON column
        2. Splits datetime column
        3. Splits location string
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

        # Step 3: Splits location string
        df = self._transform_location_split(df, ['full_name'])

        # Step 4: Filters passed records
        df = self.filter_quality_result(df)

        # Step 5: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 6: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        # Step 7: Selecting Columns that I want to take to processed layer
        df = df.select(
        'full_name_restaurant_id',
        'full_name_restaurant_name',
        'full_name',
        'u_type',
        'street',
        'city',
        'country',
        'latitude',
        'longitude',
        'sys_updated_on_dt',
        'sys_updated_on_timestamp',
        'sys_created_on_dt',
        'sys_created_on_timestamp',
        'coordinates_retrieved_on',
        'lat_long_error',
        'cmn_location_source',
        'cmn_location_type',
        'company',
        'contact',
        'u_monday_close',
        'u_monday_open',
        'u_tuesday_close',
        'u_tuesday_open',
        'u_wednesday_close',
        'u_wednesday_open',
        'u_thursday_close',
        'u_thursday_open',
        'u_friday_close',
        'u_friday_open',
        'u_saturday_close',
        'u_saturday_open',
        'u_sunday_close',
        'u_sunday_open',
        'duplicate',
        'stock_room',
        'u_drive_thru',
        'u_fit',
        'u_nlg',
        'u_otp',
        'fax_phone',
        'life_cycle_stage',
        'life_cycle_stage_status',
        'managed_by_group',
        'parent',
        'phone',
        'phone_territory',
        'primary_location',
        'state',
        'sys_created_by',
        'sys_id',
        'sys_mod_count',
        'sys_tags',
        'sys_updated_by',
        'time_zone',
        'u_adsl_line',
        'u_adsl_line2',
        'u_comms_line_contract_id',
        'u_email',
        'u_go_live',
        'u_mcduk_mccafe',
        'u_mcduk_store_number',
        'u_ownership',
        'u_projects',
        'u_restaurant_build_url',
        'u_restaurant_close_date',
        'u_rlg1',
        'u_supporting_role',
        'u_type_of_phone',
        'zip',
        'u_franchisee_regional_manager_display_value',
        'u_franchisee_regional_manager_link',
        'u_regional_manager_display_value',
        'u_regional_manager_link',
        'u_fanchisee_display_value',
        'u_fanchisee_link',
        'u_franchisees_chief_ops_manager_display_value',
        'u_franchisees_chief_ops_manager_link',
        'u_franchisees_ops_manager_display_value',
        'u_franchisees_ops_manager_link',
        'u_director_display_value',
        'u_director_link',
        'u_chief_ops_officer_display_value',
        'u_chief_ops_officer_link',
        'u_franchisees_consultant_display_value',
        'u_franchisees_consultant_link',
        'u_franchisees_director_display_value',
        'u_franchisees_director_link',
        'u_ops_manager_display_value',
        'u_ops_manager_link'
        )

        column_mapping = {

            'city': ('city', 'string'),
            'cmn_location_source': ('cmn_location_source', 'string'),
            'cmn_location_type': ('cmn_location_type', 'string'),
            'company': ('company', 'string'),
            'contact': ('contact', 'string'),
            'coordinates_retrieved_on': ('coordinates_retrieved_on', 'string'),
            'country': ('country', 'string'),
            'duplicate': ('duplicate_flag', 'boolean'),
            'fax_phone': ('fax_phone', 'string'),
            'full_name': ('restaurant_full_name', 'string'),
            'lat_long_error': ('lat_long_error', 'string'),
            'latitude': ('latitude', 'double'),
            'life_cycle_stage': ('life_cycle_stage', 'string'),
            'life_cycle_stage_status': ('life_cycle_stage_status', 'string'),
            'longitude': ('longitude', 'double'),
            'managed_by_group': ('managed_by_group', 'string'),
            'parent': ('parent', 'string'),
            'phone': ('phone', 'string'),
            'phone_territory': ('phone_territory', 'string'),
            'primary_location': ('primary_location', 'string'),
            'state': ('state', 'string'),
            'stock_room': ('stock_room_flag', 'boolean'),
            'street': ('street', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_id': ('sys_id', 'string'),
            'sys_mod_count': ('sys_mod_count', 'string'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'time_zone': ('time_zone', 'string'),
            'u_adsl_line': ('u_adsl_line', 'string'),
            'u_adsl_line2': ('u_adsl_line2', 'string'),
            'u_comms_line_contract_id': ('u_comms_line_contract_id', 'string'),
            'u_drive_thru': ('u_drive_thru_flag', 'boolean'),
            'u_email': ('u_email', 'string'),
            'u_fit': ('u_fit_flag', 'boolean'),
            'u_friday_close': ('u_friday_close', 'time'),
            'u_friday_open': ('u_friday_open', 'time'),
            'u_go_live': ('u_go_live', 'string'),
            'u_mcduk_mccafe': ('u_mcduk_mccafe', 'string'),
            'u_mcduk_store_number': ('u_mcduk_store_number', 'string'),
            'u_monday_close': ('u_monday_close', 'time'),
            'u_monday_open': ('u_monday_open', 'time'),
            'u_nlg': ('u_nlg_flag', 'boolean'),
            'u_otp': ('u_otp_flag', 'boolean'),
            'u_ownership': ('restaurant_ownership_type', 'string'),
            'u_projects': ('u_projects', 'string'),
            'u_restaurant_build_url': ('u_restaurant_build_url', 'string'),
            'u_restaurant_close_date': ('u_restaurant_close_date', 'string'),
            'u_rlg1': ('u_rlg1', 'string'),
            'u_saturday_close': ('u_saturday_close', 'time'),
            'u_saturday_open': ('u_saturday_open', 'time'),
            'u_sunday_close': ('u_sunday_close', 'time'),
            'u_sunday_open': ('u_sunday_open', 'time'),
            'u_supporting_role': ('u_supporting_role', 'string'),
            'u_thursday_close': ('u_thursday_close', 'time'),
            'u_thursday_open': ('u_thursday_open', 'time'),
            'u_tuesday_close': ('u_tuesday_close', 'time'),
            'u_tuesday_open': ('u_tuesday_open', 'time'),
            'u_type': ('location_type', 'string'),
            'u_type_of_phone': ('u_type_of_phone', 'string'),
            'u_wednesday_close': ('u_wednesday_close', 'time'),
            'u_wednesday_open': ('u_wednesday_open', 'time'),
            'zip': ('zip', 'string'),
            'u_franchisee_regional_manager_display_value': ('u_franchisee_regional_manager', 'string'),
            'u_franchisee_regional_manager_link': ('u_franchisee_regional_manager_sys_user', 'string'),
            'u_regional_manager_display_value': ('u_regional_manager', 'string'),
            'u_regional_manager_link': ('u_regional_manager_sys_user', 'string'),
            'u_fanchisee_display_value': ('u_fanchisee', 'string'),
            'u_fanchisee_link': ('u_fanchisee_sys_user', 'string'),
            'u_franchisees_chief_ops_manager_display_value': ('u_franchisees_chief_ops_manager', 'string'),
            'u_franchisees_chief_ops_manager_link': ('u_franchisees_chief_ops_manager_sys_user', 'string'),
            'u_franchisees_ops_manager_display_value': ('u_franchisees_ops_manager', 'string'),
            'u_franchisees_ops_manager_link': ('u_franchisees_ops_manager_sys_user', 'string'),
            'u_director_display_value': ('u_director', 'string'),
            'u_director_link': ('u_director_sys_user', 'string'),
            'u_chief_ops_officer_display_value': ('u_chief_ops_officer', 'string'),
            'u_chief_ops_officer_link': ('u_chief_ops_officer_sys_user', 'string'),
            'u_franchisees_consultant_display_value': ('u_franchisees_consultant', 'string'),
            'u_franchisees_consultant_link': ('u_franchisees_consultant_sys_user', 'string'),
            'u_franchisees_director_display_value': ('u_franchisees_director', 'string'),
            'u_franchisees_director_link': ('u_franchisees_director_sys_user', 'string'),
            'u_ops_manager_display_value': ('u_ops_manager', 'string'), 
            'u_ops_manager_link': ('u_ops_manager_sys_user', 'string'),      
            'sys_updated_on_dt': ('sys_updated_date', 'date'),  
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'timestamp'), 
            'sys_created_on_dt': ('sys_created_date', 'date'), 
            'sys_created_on_timestamp': ('sys_created_timestamp', 'timestamp'), 
            'full_name_restaurant_id': ('restaurant_id', 'integer'),
            'full_name_restaurant_name': ('restaurant_name', 'string')
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
            df.write.format("delta").mode("overwrite").save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('processed', 'service_now_location', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['restaurant_id','restaurant_name','sys_created_timestamp']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        # If error detected from DQ failing then will raise
        if self.sns_trigger:
            message = "Records in the error folder that have failed transformation"
            self.aws_instance.send_sns_message(message)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')




    @transformation_timer
    def _transform_location_split(self,df, column_names):
        """
        Apply the split location string transformation to specified columns in the DataFrame.

        Args:
            df (DataFrame): The input Spark DataFrame.
            column_names (list): List of column names to split location strings for.

        Returns:
            DataFrame: The DataFrame with split location columns.
        """
        self.logger.info('Running the transform_location_split function')
        for column_name in column_names:
            is_numeric = F.col(column_name).rlike("^[0-9]+$")
            is_alphabetic = F.col(column_name).rlike("^[a-zA-Z\s]+$")

            df = df.withColumn(
                f"{column_name}_restaurant_id",
                F.when(is_numeric | is_alphabetic, F.lit(-1))
                .otherwise(F.regexp_replace(F.col(column_name), r"([0-9]+)[^0-9]?.*", r"$1").cast("int"))
            ).withColumn(
                f"{column_name}_restaurant_name",
                F.when(is_numeric | is_alphabetic, F.col(column_name))
                .otherwise(F.regexp_replace(F.col(column_name), r"^[0-9]+[^0-9]?(.*)", r"$1"))
            ).withColumn(
                f"{column_name}_restaurant_name",
                F.when(F.col(f"{column_name}_restaurant_name").rlike("^[, -]"), 
                    F.regexp_replace(F.col(f"{column_name}_restaurant_name"), r"^[, -]+", ""))
                .otherwise(F.col(f"{column_name}_restaurant_name"))
            )

            df = df.withColumn(
                f"{column_name}_restaurant_id",
                F.when(F.col(f"{column_name}_restaurant_id").isNull(), F.lit(-1))
                .otherwise(F.col(f"{column_name}_restaurant_id"))
            )

        return df

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
                    SELECT full_name, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY full_name
                ) b ON a.full_name = b.full_name AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)