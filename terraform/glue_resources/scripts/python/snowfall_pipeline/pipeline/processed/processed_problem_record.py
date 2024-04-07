from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import DeltaTable


class ProcessedProblemRecord(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/problem_record"


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
        df = self.filter_quality_result(df,partition_column_drop=['created_year','created_month'])

        # Step 6: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 7: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        # Step 8: Selecting Columns that I want to take to processed layer
        df = df.select(
            'active',
            'activity_due',
            'additional_assignee_list',
            'approval',
            'approval_history',
            'approval_set',
            'business_duration_seconds',
            'calendar_duration_seconds',
            'close_notes',
            'closed_by',
            'cmdb_ci',
            'comments',
            'comments_and_work_notes',
            'company',
            'contact_type',
            'contract',
            'correlation_display',
            'correlation_id',
            'description',
            'due_date',
            'escalation',
            'expected_start',
            'follow_up',
            'group_list',
            'impact',
            'knowledge',
            'known_error',
            'made_sla',
            'major_problem',
            'number',
            'order',
            'parent',
            'priority',
            'problem_state',
            'reassignment_count',
            'related_incidents',
            'review_outcome',
            'rfc',
            'route_reason',
            'short_description',
            'skills',
            'sla_due',
            'state',
            'sys_class_name',
            'sys_created_by',
            'sys_domain_path',
            'sys_id',
            'sys_mod_count',
            'sys_tags',
            'sys_updated_by',
            'task_effective_number',
            'time_worked_seconds',
            'u_analyze_impact',
            'u_boolean_1',
            'u_business_service',
            'u_category',
            'u_follow_up',
            'u_franchisee_director',
            'u_franchisee_ops_manager',
            'u_franchisee_regional_manager',
            'u_franchisees_chief_ops_manager',
            'u_glide_date_2',
            'u_known_error',
            'u_problem_type',
            'u_rca',
            'u_string_1',
            'u_subcategory',
            'universal_request',
            'upon_approval',
            'upon_reject',
            'urgency',
            'user_input',
            'watch_list',
            'work_around',
            'work_end',
            'work_notes',
            'work_notes_list',
            'work_start',
            'created_year',
            'created_month',
            'opened_by_display_value',
            'opened_by_link',
            'sys_domain_display_value',
            'sys_domain_link',
            'u_franchisee_display_value',
            'u_franchisee_link',
            'business_service_display_value',
            'business_service_link',
            'assignment_group_display_value',
            'assignment_group_link',
            'service_offering_display_value',
            'service_offering_link',
            'u_related_kb_display_value',
            'u_related_kb_link',
            'assigned_to_display_value',
            'assigned_to_link',
            'u_franchisees_consultant_display_value',
            'u_franchisees_consultant_link',
            'location_display_value',
            'location_link',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'sys_created_on_dt',
            'sys_created_on_timestamp',
            'opened_at_dt',
            'opened_at_timestamp',
            'closed_at_dt',
            'closed_at_timestamp',
            'restaurant_full_name',
            'restaurant_name',
            'restaurant_id'
        )


        # Create a dictionary to map old column names to new column names and data types
        column_mapping = {
            'active': ('active_flag', 'boolean'),
            'activity_due': ('activity_due', 'string'),
            'additional_assignee_list': ('additional_assignee_list', 'string'),
            'approval': ('approval', 'string'),
            'approval_history': ('approval_history', 'string'),
            'approval_set': ('approval_set', 'string'),
            'business_duration_seconds': ('business_duration_seconds', 'integer'),
            'calendar_duration_seconds': ('calendar_duration_seconds', 'integer'),
            'close_notes': ('close_notes', 'string'),
            'closed_at_dt': ('closed_at_date', 'date'),
            'closed_at_timestamp': ('closed_at_timestamp', 'string'),
            'closed_by': ('closed_by', 'string'),
            'cmdb_ci': ('cmdb_ci', 'string'),
            'comments': ('comments', 'string'),
            'comments_and_work_notes': ('comments_and_work_notes', 'string'),
            'company': ('company', 'string'),
            'contact_type': ('contact_type', 'string'),
            'contract': ('contract', 'string'),
            'correlation_display': ('correlation_display', 'string'),
            'correlation_id': ('correlation_id', 'string'),
            'description': ('description', 'string'),
            'due_date': ('due_date', 'date'),
            'escalation': ('escalation', 'string'),
            'expected_start': ('expected_start', 'string'),
            'follow_up': ('follow_up', 'string'),
            'group_list': ('group_list', 'string'),
            'impact': ('impact', 'string'),
            'knowledge': ('knowledge', 'string'),
            'known_error': ('known_error_flag', 'boolean'),
            'made_sla': ('made_sla_flag', 'boolean'),
            'major_problem': ('major_problem_flag', 'boolean'),
            'number': ('problem_number', 'string'),
            'order': ('order', 'string'),
            'parent': ('parent', 'string'),
            'priority': ('priority', 'string'),
            'problem_state': ('problem_state', 'string'),
            'reassignment_count': ('reassignment_count', 'integer'),
            'related_incidents': ('related_incidents_count', 'integer'),
            'review_outcome': ('review_outcome', 'string'),
            'rfc': ('rfc', 'string'),
            'route_reason': ('route_reason', 'string'),
            'short_description': ('short_description', 'string'),
            'skills': ('skills', 'string'),
            'sla_due': ('sla_due', 'string'),
            'state': ('state', 'string'),
            'sys_class_name': ('sys_class_name', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_domain_path': ('sys_domain_path', 'string'),
            'sys_id': ('sys_id', 'string'),
            'sys_mod_count': ('sys_mod_count', 'integer'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'task_effective_number': ('task_effective_number', 'string'),
            'time_worked_seconds': ('time_worked_seconds', 'integer'),
            'u_analyze_impact': ('u_analyze_impact', 'string'),
            'u_boolean_1': ('u_boolean_flag', 'boolean'),
            'u_business_service': ('u_business_service', 'string'),
            'u_category': ('u_category', 'string'),
            'u_follow_up': ('u_follow_up', 'string'),
            'u_franchisee_director': ('u_franchisee_director', 'string'),
            'u_franchisee_ops_manager': ('u_franchisee_ops_manager', 'string'),
            'u_franchisee_regional_manager': ('u_franchisee_regional_manager', 'string'),
            'u_franchisees_chief_ops_manager': ('u_franchisees_chief_ops_manager', 'string'),
            'u_glide_date_2': ('u_glide_date', 'date'),
            'u_known_error': ('u_known_error_flag', 'boolean'),
            'u_problem_type': ('u_problem_type', 'string'),
            'u_rca': ('u_rca', 'string'),
            'u_string_1': ('u_string_1', 'string'),
            'u_subcategory': ('u_subcategory', 'string'),
            'universal_request': ('universal_request', 'string'),
            'upon_approval': ('upon_approval', 'string'),
            'upon_reject': ('upon_reject', 'string'),
            'urgency': ('urgency', 'string'),
            'user_input': ('user_input', 'string'),
            'watch_list': ('watch_list', 'string'),
            'work_around': ('work_around', 'string'),
            'work_end': ('work_end', 'string'),
            'work_notes': ('work_notes', 'string'),
            'work_notes_list': ('work_notes_list', 'string'),
            'work_start': ('work_start', 'string'),
            'created_year': ('created_year', 'int'),
            'created_month': ('created_month', 'int'),
            'opened_by_display_value': ('opened_by', 'string'),
            'opened_by_link': ('opened_by_sys_user_link', 'string'),
            'sys_domain_display_value': ('sys_domain', 'string'),
            'sys_domain_link': ('sys_domain_link', 'string'),
            'u_franchisee_display_value': ('u_franchisee', 'string'),
            'u_franchisee_link': ('u_franchisee_link', 'string'),
            'business_service_display_value': ('business_service', 'string'),
            'business_service_link': ('business_service_cmdb_link', 'string'),
            'assignment_group_display_value': ('assignment_group', 'string'),
            'assignment_group_link': ('assignment_group_sys_user_group_link', 'string'),
            'service_offering_display_value': ('service_offering', 'string'),
            'service_offering_link': ('service_offering_offering_link', 'string'),
            'u_related_kb_display_value': ('u_related_kb', 'string'),
            'u_related_kb_link': ('u_related_kb_kb_link', 'string'),
            'assigned_to_display_value': ('assigned_to', 'string'),
            'assigned_to_link': ('assigned_to_sys_user_link', 'string'),
            'u_franchisees_consultant_display_value': ('u_franchisees_consultant', 'string'),
            'u_franchisees_consultant_link': ('u_franchisees_consultant_sys_user_link', 'string'),
            'location_display_value': ('location', 'string'),
            'location_link': ('location_link', 'string'),
            'sys_updated_on_dt': ('sys_updated_on_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_on_timestamp', 'string'),
            'sys_created_on_dt': ('sys_created_on_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_on_timestamp', 'string'),
            'opened_at_dt': ('opened_at_date', 'date'),
            'opened_at_timestamp': ('opened_at_timestamp', 'string'),
            'restaurant_full_name': ('restaurant_full_name', 'string'),
            'restaurant_name': ('restaurant_name', 'string'),
            'restaurant_id': ('restaurant_id', 'integer')
        }


        # Step 9: Changes column names and schema
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
                self.aws_instance.create_athena_delta_table('processed', 'service_now_problem_record', save_output_path, self.athena_output_path)
                
            else:

                # Merge data to the Delta table
                merge_columns = ['problem_number','sys_created_on_timestamp','created_year','created_month']
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
                    SELECT number, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY number
                ) b ON a.number = b.number AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)