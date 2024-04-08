from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import DeltaTable


class ProcessedServiceRequest(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/service_request"


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
        df = self.split_datetime_column(df,self.pipeline_config.get('process_timestamp'))


        # Step 3: Filters passed records
        df = self.filter_quality_result(df,partition_column_drop=['created_year','created_month'])

        # Step 4: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 5: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        # Step 6: Selecting Columns that I want to take to processed layer
        df = df.select (
            'active',
            'activity_due',
            'additional_assignee_list',
            'approval',
            'approval_history',
            'backordered',
            'billable',
            'business_duration',
            'calendar_duration',
            'close_notes',
            'cmdb_ci',
            'comments',
            'comments_and_work_notes',
            'company',
            'configuration_item',
            'contact_type',
            'contract',
            'correlation_display',
            'correlation_id',
            'description',
            'escalation',
            'estimated_delivery',
            'expected_start',
            'follow_up',
            'group_list',
            'impact',
            'knowledge',
            'location',
            'made_sla',
            'number',
            'order',
            'parent',
            'price',
            'priority',
            'quantity',
            'reassignment_count',
            'recurring_frequency',
            'recurring_price',
            'route_reason',
            'sc_catalog',
            'service_offering',
            'short_description',
            'skills',
            'sla_due',
            'stage',
            'state',
            'sys_class_name',
            'sys_created_by',
            'sys_domain_path',
            'sys_id',
            'sys_mod_count',
            'sys_tags',
            'sys_updated_by',
            'task_effective_number',
            'time_worked',
            'u_franchisee',
            'u_franchisee_director',
            'u_franchisee_ops_manager',
            'u_franchisee_regional_manager',
            'u_franchisees_chief_ops_manager',
            'u_franchisees_consultant',
            'u_original_incident_number',
            'universal_request',
            'upon_approval',
            'upon_reject',
            'urgency',
            'user_input',
            'watch_list',
            'work_end',
            'work_notes',
            'work_notes_list',
            'work_start',
            'created_year',
            'created_month',
            'requested_for_display_value',
            'requested_for_link',
            'opened_by_display_value',
            'opened_by_link',
            'sys_domain_display_value',
            'sys_domain_link',
            'order_guide_display_value',
            'order_guide_link',
            'request_display_value',
            'request_link',
            'assignment_group_display_value',
            'assignment_group_link',
            'closed_by_display_value',
            'closed_by_link',
            'assigned_to_display_value',
            'assigned_to_link',
            'cat_item_display_value',
            'cat_item_link',
            'business_service_display_value',
            'business_service_link',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'sys_created_on_dt',
            'sys_created_on_timestamp',
            'closed_at_dt',
            'closed_at_timestamp',
            'opened_at_dt',
            'opened_at_timestamp',
            'approval_set_dt',
            'approval_set_timestamp',
            'due_date_dt',
            'due_date_timestamp'
            )

        column_mapping = {
            'number': ('request_number', 'string'),
           'sys_created_on_dt': ('sys_created_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_timestamp', 'string'),
            'sys_updated_on_dt': ('sys_updated_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
            'opened_at_dt': ('opened_date', 'date'),
            'opened_at_timestamp': ('opened_timestamp', 'string'),
            'closed_at_dt': ('closed_date', 'date'),
            'closed_at_timestamp': ('closed_timestamp', 'string'),
            'approval_set_dt': ('approval_date', 'date'),
            'approval_set_timestamp': ('approval_timestamp', 'string'),
            'due_date_dt': ('due_date', 'date'),
            'due_date_timestamp': ('due_date_timestamp', 'string'),
            'active': ('active_flag', 'boolean'),
            'activity_due': ('activity_due', 'string'),
            'additional_assignee_list': ('additional_assignee_list', 'string'),
            'approval': ('approval', 'string'),
            'approval_history': ('approval_history', 'string'),
            'backordered': ('backordered', 'string'),
            'billable': ('billable', 'string'),
            'business_duration': ('business_duration', 'string'),
            'calendar_duration': ('calendar_duration', 'string'),
            'close_notes': ('close_notes', 'string'),
            'cmdb_ci': ('cmdb_ci', 'string'),
            'comments': ('comments', 'string'),
            'comments_and_work_notes': ('comments_and_work_notes', 'string'),
            'company': ('company', 'string'),
            'configuration_item': ('configuration_item', 'string'),
            'contact_type': ('contact_type', 'string'),
            'contract': ('contract', 'string'),
            'correlation_display': ('correlation_display', 'string'),
            'correlation_id': ('correlation_id', 'string'),
            'description': ('description', 'string'),
            'escalation': ('escalation', 'string'),
            'estimated_delivery': ('estimated_delivery', 'string'),
            'expected_start': ('expected_start', 'string'),
            'follow_up': ('follow_up', 'string'),
            'group_list': ('group_list', 'string'),
            'impact': ('impact', 'string'),
            'knowledge': ('knowledge', 'string'),
            'location': ('location', 'string'),
            'made_sla': ('made_sla', 'string'),
            'order': ('order', 'string'),
            'parent': ('parent', 'string'),
            'price': ('price', 'string'),
            'priority': ('priority', 'string'),
            'quantity': ('quantity', 'integer'),
            'reassignment_count': ('reassignment_count', 'int'),
            'recurring_frequency': ('recurring_frequency', 'string'),
            'recurring_price': ('recurring_price', 'string'),
            'route_reason': ('route_reason', 'string'),
            'sc_catalog': ('sc_catalog', 'string'),
            'service_offering': ('service_offering', 'string'),
            'short_description': ('short_description', 'string'),
            'skills': ('skills', 'string'),
            'sla_due': ('sla_due', 'string'),
            'stage': ('stage', 'string'),
            'state': ('state', 'string'),
            'sys_class_name': ('sys_class_name', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_domain_path': ('sys_domain_path', 'string'),
            'sys_id': ('sys_id', 'string'),
            'sys_mod_count': ('sys_mod_count', 'integer'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'task_effective_number': ('task_effective_number', 'string'),
            'time_worked': ('time_worked', 'string'),
            'u_franchisee': ('u_franchisee', 'string'),
            'u_franchisee_director': ('u_franchisee_director', 'string'),
            'u_franchisee_ops_manager': ('u_franchisee_ops_manager', 'string'),
            'u_franchisee_regional_manager': ('u_franchisee_regional_manager', 'string'),
            'u_franchisees_chief_ops_manager': ('u_franchisees_chief_ops_manager', 'string'),
            'u_franchisees_consultant': ('u_franchisees_consultant', 'string'),
            'u_original_incident_number': ('u_original_incident_number', 'string'),
            'universal_request': ('universal_request', 'string'),
            'upon_approval': ('upon_approval', 'string'),
            'upon_reject': ('upon_reject', 'string'),
            'urgency': ('urgency', 'string'),
            'user_input': ('user_input', 'string'),
            'watch_list': ('watch_list', 'string'),
            'work_end': ('work_end', 'string'),
            'work_notes': ('work_notes', 'string'),
            'work_notes_list': ('work_notes_list', 'string'),
            'work_start': ('work_start', 'string'),
            'created_year': ('created_year', 'int'),
            'created_month': ('created_month', 'int'),
            'requested_for_display_value': ('requested_for', 'string'),
            'requested_for_link': ('requested_for_sys_user_link', 'string'),
            'opened_by_display_value': ('opened_by', 'string'),
            'opened_by_link': ('opened_by_sys_user_link', 'string'),
            'sys_domain_display_value': ('sys_domain', 'string'),
            'sys_domain_link': ('sys_domain_sys_user_link', 'string'),
            'order_guide_display_value': ('order_guide', 'string'),
            'order_guide_link': ('order_guide_link', 'string'),
            'request_display_value': ('request', 'string'),
            'request_link': ('request_sc_request_link', 'string'),
            'assignment_group_display_value': ('assignment_group', 'string'),
            'assignment_group_link': ('assignment_group_link', 'string'),
            'closed_by_display_value': ('closed_by', 'string'),
            'closed_by_link': ('closed_by_sys_user_link', 'string'),
            'assigned_to_display_value': ('assigned_to', 'string'),
            'assigned_to_link': ('assigned_to_sys_user_link', 'string'),
            'cat_item_display_value': ('cat_item', 'string'),
            'cat_item_link': ('cat_item_link', 'string'),
            'business_service_display_value': ('business_service', 'string'),
            'business_service_link': ('business_service_link', 'string')
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
                self.aws_instance.create_athena_delta_table('processed', 'service_now_service_request', save_output_path, self.athena_output_path)
                
            else:

                # Merge data to the Delta table
                merge_columns = ['request_number','sys_created_timestamp','created_year','created_month']
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