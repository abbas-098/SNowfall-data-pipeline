from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import DeltaTable
from pyspark.sql.functions import broadcast, udf, explode, lit, array
from pyspark.sql.types import ArrayType, StringType


class ProcessedChangeRequest(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "50") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/change_request"


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
        4. Exploding the columns
        5. Joining with location table
        6. Filters passed records
        7. Gets unique records
        8. Drops unnecessary columns
        9. Selecting columns to take to processed layer
        10. Change column names and schema.

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
        
        # Step 4: Explode U_site column
        df = self.process_change_request(df,'u_site')

        # Step 5: Joining with location table
        df = self.join_location_table(df,'u_site')

        # Step 6: Filters passed records
        df = self.filter_quality_result(df,partition_column_drop=['created_year','created_month'])

        # Step 7: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 8: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        # Step 9: Selecting Columns that I want to take to processed layer
        df = df.select(
            'active',
            'activity_due',
            'additional_assignee_list',
            'approval',
            'approval_history',
            'backout_plan',
            'cab_date',
            'cab_recommendation',
            'cab_required',
            'category',
            'change_plan',
            'chg_model',
            'close_code',
            'close_notes',
            'comments',
            'comments_and_work_notes',
            'company',
            'conflict_status',
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
            'implementation_plan',
            'justification',
            'knowledge',
            'location',
            'made_sla',
            'number',
            'on_hold',
            'on_hold_reason',
            'on_hold_task',
            'order',
            'outside_maintenance_schedule',
            'parent',
            'phase',
            'phase_state',
            'priority',
            'production_system',
            'reason',
            'reassignment_count',
            'requested_by_date',
            'review_comments',
            'review_date',
            'review_status',
            'risk',
            'risk_impact_analysis',
            'route_reason',
            'scope',
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
            'test_plan',
            'type',
            'u_all_corporate_impacted',
            'u_all_impacted',
            'u_all_restaurant_impacted',
            'u_choice_1',
            'u_franchisee',
            'u_franchisee_director',
            'u_franchisee_ops_manager',
            'u_franchisee_regional_manager',
            'u_franchisees_chief_ops_manager',
            'u_franchisees_consultant',
            'u_has_this_change_been_agreed',
            'u_impacted_area',
            'u_information',
            'u_internal_external_comms_required',
            'u_is_there_a_deployment_ticket_raised',
            'u_mirror_this_change',
            'u_outage',
            'u_outage_reason',
            'u_please_provide_the_deployment_ticket_number',
            'u_provide_the_chg_reference_of_the_deployment_to_the_techseed_restaurants',
            'u_string_2',
            'u_supplier_involvement',
            'u_techseed_restaurants_have_been_deployed_here',
            'u_which_all_mcdonald_s_business_parties',
            'u_who_will_publish_themm',
            'u_whom_they_have_been_engage',
            'unauthorized',
            'universal_request',
            'upon_approval',
            'upon_reject',
            'urgency',
            'user_input',
            'watch_list',
            'work_notes',
            'work_notes_list',
            'created_year',
            'created_month',
            'assignment_group_display_value',
            'assignment_group_link',
            'business_service_display_value',
            'business_service_link',
            'cab_delegate_display_value',
            'cab_delegate_link',
            'closed_by_display_value',
            'closed_by_link',
            'cmdb_ci_display_value',
            'cmdb_ci_link',
            'opened_by_display_value',
            'opened_by_link',
            'requested_by_display_value',
            'requested_by_link',
            'service_offering_display_value',
            'service_offering_link',
            'std_change_producer_version_display_value',
            'std_change_producer_version_link',
            'sys_domain_display_value',
            'sys_domain_link',
            'u_change_implementer_display_value',
            'u_change_implementer_link',
            'assigned_to_display_value',
            'assigned_to_link',
            'business_duration_seconds',
            'calendar_duration_seconds',
            'time_worked_seconds',
            'sys_created_on_dt',
            'sys_created_on_timestamp',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'approval_set_dt',
            'approval_set_timestamp',
            'end_date_dt',
            'end_date_timestamp',
            'work_start_dt',
            'work_start_timestamp',
            'start_date_dt',
            'start_date_timestamp',
            'closed_at_dt',
            'closed_at_timestamp',
            'opened_at_dt',
            'opened_at_timestamp',
            'work_end_dt',
            'work_end_timestamp',
            'cab_date_time_dt',
            'cab_date_time_timestamp',
            'conflict_last_run_dt',
            'conflict_last_run_timestamp',
            'restaurant_full_name',
            'restaurant_name',
            'restaurant_id'
        )

        # Step 10. Changes column names and schema
        column_mapping = {
            'number': ('change_request_number', 'string'),
            'restaurant_full_name': ('restaurant_full_name', 'string'),
            'restaurant_name': ('restaurant_name', 'string'),
            'restaurant_id': ('restaurant_id', 'integer'),
            'sys_created_on_dt': ('sys_created_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_timestamp', 'string'),
            'sys_updated_on_dt': ('sys_updated_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
            'business_duration_seconds': ('business_duration_seconds', 'integer'),
            'calendar_duration_seconds': ('calendar_duration_seconds', 'integer'),
            'time_worked_seconds': ('time_worked_seconds', 'integer'),
            'approval_set_dt': ('approval_set_date', 'date'),
            'approval_set_timestamp': ('approval_set_timestamp', 'string'),
            'end_date_dt': ('end_date_date', 'date'),
            'end_date_timestamp': ('end_date_timestamp', 'string'),
            'work_start_dt': ('work_start_date', 'date'),
            'work_start_timestamp': ('work_start_timestamp', 'string'),
            'start_date_dt': ('start_date', 'date'),
            'start_date_timestamp': ('start_date_timestamp', 'string'),
            'closed_at_dt': ('closed_date', 'date'),
            'closed_at_timestamp': ('closed_timestamp', 'string'),
            'opened_at_dt': ('opened_date', 'date'),
            'opened_at_timestamp': ('opened_timestamp', 'string'),
            'work_end_dt': ('work_end_date', 'date'),
            'work_end_timestamp': ('work_end_timestamp', 'string'),
            'active': ('active_flag', 'boolean'),
            'activity_due': ('activity_due', 'string'),
            'additional_assignee_list': ('additional_assignee_list', 'string'),
            'approval': ('approval', 'string'),
            'approval_history': ('approval_history', 'string'),
            'backout_plan': ('backout_plan', 'string'),
            'cab_date': ('cab_date', 'date'),
            'cab_recommendation': ('cab_recommendation', 'string'),
            'cab_required': ('cab_required_flag', 'boolean'),
            'category': ('category', 'string'),
            'change_plan': ('change_plan', 'string'),
            'chg_model': ('chg_model', 'string'),
            'close_code': ('close_code', 'string'),
            'close_notes': ('close_notes', 'string'),
            'comments': ('comments', 'string'),
            'comments_and_work_notes': ('comments_and_work_notes', 'string'),
            'company': ('company', 'string'),
            'conflict_status': ('conflict_status', 'string'),
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
            'implementation_plan': ('implementation_plan', 'string'),
            'justification': ('justification', 'string'),
            'knowledge': ('knowledge', 'string'),
            'location': ('location', 'string'),
            'made_sla': ('made_sla_flag', 'boolean'),
            'on_hold': ('on_hold_flag', 'boolean'),
            'on_hold_reason': ('on_hold_reason', 'string'),
            'on_hold_task': ('on_hold_task', 'string'),
            'order': ('order', 'string'),
            'outside_maintenance_schedule': ('outside_maintenance_schedule', 'string'),
            'parent': ('parent', 'string'),
            'phase': ('phase', 'string'),
            'phase_state': ('phase_state', 'string'),
            'priority': ('priority', 'string'),
            'production_system': ('production_system', 'string'),
            'reason': ('reason', 'string'),
            'reassignment_count': ('reassignment_count', 'integer'),
            'requested_by_date': ('requested_by_date', 'date'),
            'review_comments': ('review_comments', 'string'),
            'review_date': ('review_date', 'date'),
            'review_status': ('review_status', 'string'),
            'risk': ('risk', 'string'),
            'risk_impact_analysis': ('risk_impact_analysis', 'string'),
            'route_reason': ('route_reason', 'string'),
            'scope': ('scope', 'string'),
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
            'test_plan': ('test_plan', 'string'),
            'type': ('type', 'string'),
            'u_all_corporate_impacted': ('u_all_corporate_impacted', 'string'),
            'u_all_impacted': ('u_all_impacted', 'string'),
            'u_all_restaurant_impacted': ('u_all_restaurant_impacted', 'string'),
            'u_choice_1': ('u_choice_1', 'string'),
            'u_franchisee': ('u_franchisee', 'string'),
            'u_franchisee_director': ('u_franchisee_director', 'string'),
            'u_franchisee_ops_manager': ('u_franchisee_ops_manager', 'string'),
            'u_franchisee_regional_manager': ('u_franchisee_regional_manager', 'string'),
            'u_franchisees_chief_ops_manager': ('u_franchisees_chief_ops_manager', 'string'),
            'u_franchisees_consultant': ('u_franchisees_consultant', 'string'),
            'u_has_this_change_been_agreed': ('u_has_this_change_been_agreed', 'string'),
            'u_impacted_area': ('u_impacted_area', 'string'),
            'u_information': ('u_information', 'string'),
            'u_internal_external_comms_required': ('u_internal_external_comms_required', 'string'),
            'u_is_there_a_deployment_ticket_raised': ('u_is_there_a_deployment_ticket_raised', 'string'),
            'u_mirror_this_change': ('u_mirror_this_change', 'string'),
            'u_outage': ('u_outage', 'string'),
            'u_outage_reason': ('u_outage_reason', 'string'),
            'u_please_provide_the_deployment_ticket_number': ('u_please_provide_the_deployment_ticket_number', 'string'),
            'u_provide_the_chg_reference_of_the_deployment_to_the_techseed_restaurants': ('u_provide_the_chg_reference_of_the_deployment_to_the_techseed_restaurants', 'string'),
            'u_string_2': ('u_string_2', 'string'),
            'u_supplier_involvement': ('u_supplier_involvement', 'string'),
            'u_techseed_restaurants_have_been_deployed_here': ('u_techseed_restaurants_have_been_deployed_here', 'string'),
            'u_which_all_mcdonald_s_business_parties': ('u_which_all_mcdonald_s_business_parties', 'string'),
            'u_who_will_publish_themm': ('u_who_will_publish_themm', 'string'),
            'u_whom_they_have_been_engage': ('u_whom_they_have_been_engage', 'string'),
            'unauthorized': ('unauthorized', 'string'),
            'universal_request': ('universal_request', 'string'),
            'upon_approval': ('upon_approval', 'string'),
            'upon_reject': ('upon_reject', 'string'),
            'urgency': ('urgency', 'string'),
            'user_input': ('user_input', 'string'),
            'watch_list': ('watch_list', 'string'),
            'work_notes': ('work_notes', 'string'),
            'work_notes_list': ('work_notes_list', 'string'),
            'created_year': ('created_year', 'integer'),
            'created_month': ('created_month', 'integer'),
            'assignment_group_display_value': ('assignment_group', 'string'),
            'assignment_group_link': ('assignment_group_sys_user_link', 'string'),
            'business_service_display_value': ('business_service', 'string'),
            'business_service_link': ('business_service_cmdb_link', 'string'),
            'cab_delegate_display_value': ('cab_delegate', 'string'),
            'cab_delegate_link': ('cab_delegate_link', 'string'),
            'closed_by_display_value': ('closed_by', 'string'),
            'closed_by_link': ('closed_by_link', 'string'),
            'cmdb_ci_display_value': ('cmdb_ci', 'string'),
            'cmdb_ci_link': ('cmdb_ci_link', 'string'),
            'opened_by_display_value': ('opened_by', 'string'),
            'opened_by_link': ('opened_by_link', 'string'),
            'requested_by_display_value': ('requested', 'string'),
            'requested_by_link': ('requested_by_link', 'string'),
            'service_offering_display_value': ('service_offering', 'string'),
            'service_offering_link': ('service_offering_link', 'string'),
            'std_change_producer_version_display_value': ('std_change_producer_version', 'string'),
            'std_change_producer_version_link': ('std_change_producer_version_link', 'string'),
            'sys_domain_display_value': ('sys_domain', 'string'),
            'sys_domain_link': ('sys_domain_link', 'string'),
            'u_change_implementer_display_value': ('u_change_implementer', 'string'),
            'u_change_implementer_link': ('u_change_implementer_link', 'string'),
            'assigned_to_display_value': ('assigned_to', 'string'),
            'assigned_to_link': ('assigned_to_link', 'string'),
            'cab_date_time_dt': ('cab_date_time_date', 'date'),
            'cab_date_time_timestamp': ('cab_date_time_timestamp', 'string'),
            'conflict_last_run_dt': ('conflict_last_run_date', 'date'),
            'conflict_last_run_timestamp': ('conflict_last_run_timestamp', 'string')
        }

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
                self.aws_instance.create_athena_delta_table('processed', 'service_now_change_request', save_output_path, self.athena_output_path)
                
            else:

                # Merge data to the Delta table
                merge_columns = ['change_request_number','sys_created_timestamp','restaurant_full_name','created_year','created_month']
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
                    SELECT number,u_site, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY number,u_site
                ) b ON a.number = b.number AND 
                       a.u_site = b.u_site AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)
    

    @transformation_timer
    def process_change_request(self,df, column_name):
        """
        Process change request by exploding the DataFrame based on the provided column_name and joining it with location data.

        Args:
            df (DataFrame): The input DataFrame.
            column_name (str): The name of the column to process.

        Returns:
            DataFrame: The processed DataFrame.
        """
        # Extracting all stores with a comma in their name from locations DataFrame
        location_df = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/service_now/location/")
        filtered_rows = location_df.filter(location_df['restaurant_full_name'].contains(','))
        filtered_rows = filtered_rows.select("restaurant_full_name").collect()
        all_store_commas = array([lit(row['restaurant_full_name']) for row in filtered_rows])

        # Broadcast the location DataFrame to all worker nodes
        broadcasted_location_df = broadcast(location_df.select("restaurant_full_name", "restaurant_id", "restaurant_name"))

        # Define UDF to generate combined list
        @udf(ArrayType(StringType()))
        def generate_combined_list_udf(input_string, all_store_commas):
            """
            Generate a combined list of strings based on input_string and a list of strings.

            Args:
                input_string (str): The input string.
                all_store_commas (list): List of strings.

            Returns:
                list: Combined list of strings.
            """
            # Initialize the output list
            output_list = []

            # Iterate through the list of items
            for item in all_store_commas:
                # Count the occurrences of the item in the input string
                count = input_string.count(item)

                # Append the item to the output list 'count' number of times
                output_list.extend([item] * count)

                # Remove the occurrences of the item from the input_string
                input_string = input_string.replace(item, '', count)

            # Split the remaining input_string by commas, strip whitespace, and filter empty strings
            new_input_string_list = [item.strip() for item in input_string.split(',') if item.strip()]

            # Combine the two lists to get the desired output list
            combined_list = new_input_string_list + output_list

            return combined_list

        # Create a new column '_new' with the same value as the specified column
        df = df.withColumn(f"{column_name}_new", generate_combined_list_udf(df[column_name], all_store_commas))

        # Explode the column_new array into rows
        df = df.drop(column_name)
        df = df.withColumn(column_name, explode(df[f"{column_name}_new"]))
        df = df.drop(f"{column_name}_new")

        # Join with location data
        processed_df = df.join(broadcasted_location_df, df[column_name] == broadcasted_location_df["restaurant_full_name"], "left")

        columns_drop = ['restaurant_full_name','restaurant_id','restaurant_name']
        processed_df = processed_df.drop(*columns_drop)

        return processed_df