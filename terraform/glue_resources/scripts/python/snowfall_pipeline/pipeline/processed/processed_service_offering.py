from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.decorators import transformation_timer
from delta.tables import DeltaTable


class ProcessedServiceOffering(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/service_offering"


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

        return df

        # Step 6: Selecting Columns that I want to take to processed layer
        df = df = df.select(
        'restaurant_id',
        'restaurant_name',
        'number',
        'short_description',
        'state',
        'sys_created_on_dt',
        'sys_created_on_timestamp',
        'opened_at_dt',
        'opened_at_timestamp',
        'resolved_at_dt',
        'resolved_at_timestamp',
        'closed_at_dt',
        'closed_at_timestamp',
        'work_end_dt',
        'work_end_timestamp',
        'business_duration_seconds',
        'priority',
        'category',
        'subcategory',
        'group_list',
        'service_offering_display_value',
        'service_offering_link',
        'u_vendor_display_value',
        'u_vendor_link',
        'u_happysignal_feedback_number',
        'u_happysignal_score',
        'activity_due',
        'active',
        'additional_assignee_list',
        'approval',
        'approval_history',
        'approval_set',
        'business_impact',
        'business_stc',
        'calendar_stc',
        'cause',
        'caused_by',
        'child_incidents',
        'close_code',
        'close_notes',
        'comments',
        'comments_and_work_notes',
        'contact_type',
        'contract',
        'correlation_display',
        'correlation_id',
        'description',
        'due_date',
        'escalation',
        'expected_start',
        'follow_up',
        'hold_reason',
        'impact',
        'incident_state',
        'knowledge',
        'made_sla',
        'notify',
        'order',
        'origin_id',
        'origin_table',
        'parent',
        'reassignment_count',
        'reopen_count',
        'route_reason',
        'severity',
        'skills',
        'sla_due',
        'sys_class_name',
        'sys_created_by',
        'sys_domain_path',
        'sys_id',
        'sys_mod_count',
        'sys_tags',
        'sys_updated_by',
        'task_effective_number',
        'time_worked',
        'u_alert_id',
        'u_aws_callback_number',
        'u_aws_scheduled',
        'u_c3_driver_files',
        'u_c3_driver_files_attached',
        'u_call_reference',
        'u_chase_in_contact_count',
        'u_chase_out_contact_count',
        'u_chat_count',
        'u_customer_escalated',
        'u_daisy_dispatched',
        'u_daisy_offsite',
        'u_daisy_onsite',
        'u_daisy_resolved',
        'u_daisy_ticket',
        'u_details_of_the_impact',
        'u_email_count',
        'u_first_time_fix',
        'u_go_live',
        'u_incident_from_chat',
        'u_incident_type',
        'u_is_this_store_high_value_contactless',
        'u_issue_type',
        'u_ka_attached',
        'u_major',
        'u_master',
        'u_native_ui',
        'u_notstrikebutton',
        'u_o2_ticket',
        'u_phone_count',
        'u_projects',
        'u_remove_engineer_visit',
        'u_resolution_confirmation',
        'u_sdm_escalated',
        'u_sdm_escalation_time',
        'u_strike_count_new',
        'u_strike_visibility',
        'u_strikebuttonclick',
        'u_sub_resolution_code',
        'u_supporting_role',
        'u_survey_triggered',
        'u_techsee_agentlink',
        'u_techsee_assistance_1',
        'u_techsee_authentication_id',
        'u_techsee_customerlink',
        'u_techsee_session',
        'u_techsee_session_id_url',
        'u_techsee_sessionid',
        'u_total_strike_count',
        'u_trade_impact',
        'u_vendor_not_found',
        'u_vendor_ticket',
        'u_vista_id',
        'u_walkin_count',
        'universal_request',
        'upon_approval',
        'upon_reject',
        'urgency',
        'user_input',
        'watch_list',
        'work_notes',
        'work_notes_list',
        'work_start',
        'created_year',
        'created_month',
        'u_franchisee_regional_manager_display_value',
        'u_franchisee_regional_manager_link',
        'u_franchisees_consultant_display_value',
        'u_franchisees_consultant_link',
        'u_franchisee_director_display_value',
        'u_franchisee_director_link',
        'cmdb_ci_display_value',
        'cmdb_ci_link',
        'u_connect_chat_display_value',
        'u_connect_chat_link',
        'closed_by_display_value',
        'closed_by_link',
        'parent_incident_display_value',
        'parent_incident_link',
        'reopened_by_display_value',
        'reopened_by_link',
        'assigned_to_display_value',
        'assigned_to_link',
        'u_reopen_detail_display_value',
        'u_reopen_detail_link',
        'u_chased_out_detail_display_value',
        'u_chased_out_detail_link',
        'u_franchisee_ops_manager_display_value',
        'u_franchisee_ops_manager_link',
        'resolved_by_display_value',
        'resolved_by_link',
        'opened_by_display_value',
        'opened_by_link',
        'sys_domain_display_value',
        'sys_domain_link',
        'u_franchisee_display_value',
        'u_franchisee_link',
        'u_franchisees_chief_ops_manager_display_value',
        'u_franchisees_chief_ops_manager_link',
        'u_strike_details_display_value',
        'u_strike_details_link',
        'business_service_display_value',
        'business_service_link',
        'caller_id_display_value',
        'caller_id_link',
        'u_related_kb_article_display_value',
        'u_related_kb_article_link',
        'assignment_group_display_value',
        'assignment_group_link',
        'u_last_assignment_group_display_value',
        'u_last_assignment_group_link',
        'problem_id_display_value',
        'problem_id_link',
        'company_display_value',
        'company_link',
        'u_chased_in_detail_display_value',
        'u_chased_in_detail_link',
        'location_display_value',
        'location_link',
        'rfc_display_value',
        'rfc_link',
        'u_time_store_may_close_seconds',
        'u_time_dt_may_close_seconds',
        'u_time_store_ok2operate_seconds',
        'u_time_dt_down_seconds',
        'u_time_dt_ok2operate_seconds',
        'calendar_duration_seconds',
        'u_time_store_down_seconds',
        'sys_updated_on_dt',
        'sys_updated_on_timestamp',
        'u_vista_dispatched_eta_dt',
        'u_vista_dispatched_eta_timestamp',
        'u_vista_offsite_dt',
        'u_vista_offsite_timestamp',
        'u_vista_onsite_dt',
        'u_vista_onsite_timestamp',
        'u_call_back_time_dt',
        'u_call_back_time_timestamp',
        'u_last_assignment_time_dt',
        'u_last_assignment_time_timestamp',
        'u_techsee_agent_session_start_time_dt',
        'u_techsee_agent_session_start_time_timestamp',
        'u_l1_5_assignment_time_dt',
        'u_l1_5_assignment_time_timestamp',
        'u_customer_escalation_time_dt',
        'u_customer_escalation_time_timestamp',
        'reopened_time_dt',
        'reopened_time_timestamp',
        'u_reopen_date_time_dt',
        'u_reopen_date_time_timestamp'
        )

        column_mapping = {
        'active': ('active_flag', 'boolean'),
        'activity_due': ('activity_due', 'string'),
        'additional_assignee_list': ('additional_assignee_list', 'string'),
        'approval': ('approval', 'string'),
        'approval_history': ('approval_history', 'string'),
        'approval_set': ('approval_set', 'string'),
        'business_impact': ('business_impact', 'string'),
        'business_stc': ('business_stc', 'string'),
        'calendar_stc': ('calendar_stc', 'string'),
        'category': ('category', 'string'),
        'cause': ('cause', 'string'),
        'caused_by': ('caused_by', 'string'),
        'child_incidents': ('child_incidents_count', 'integer'),
        'close_code': ('close_code', 'string'),
        'close_notes': ('close_notes', 'string'),
        'comments': ('comments', 'string'),
        'comments_and_work_notes': ('comments_and_work_notes', 'string'),
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
        'hold_reason': ('hold_reason', 'string'),
        'impact': ('impact', 'string'),
        'incident_state': ('incident_state', 'string'),
        'knowledge': ('knowledge_flag', 'boolean'),
        'made_sla': ('made_sla_flag', 'boolean'),
        'notify': ('notify', 'string'),
        'number': ('incident_number', 'string'),
        'order': ('order', 'string'),
        'origin_id': ('origin_id', 'string'),
        'origin_table': ('origin_table', 'string'),
        'parent': ('parent', 'string'),
        'priority': ('priority', 'string'),
        'reassignment_count': ('reassignment_count', 'integer'),
        'reopen_count': ('reopen_count', 'integer'),
        'route_reason': ('route_reason', 'string'),
        'severity': ('severity', 'string'),
        'short_description': ('short_description', 'string'),
        'skills': ('skills', 'string'),
        'sla_due': ('sla_due', 'string'),
        'state': ('state', 'string'),
        'subcategory': ('subcategory', 'string'),
        'sys_class_name': ('sys_class_name', 'string'),
        'sys_created_by': ('sys_created_by', 'string'),
        'sys_domain_path': ('sys_domain_path', 'string'),
        'sys_id': ('sys_id', 'string'),
        'sys_mod_count': ('sys_mod_count', 'integer'),
        'sys_tags': ('sys_tags', 'string'),
        'sys_updated_by': ('sys_updated_by', 'string'),
        'task_effective_number': ('task_effective_number', 'string'),
        'time_worked': ('time_worked', 'string'),
        'u_alert_id': ('u_alert_id', 'string'),
        'u_aws_callback_number': ('u_aws_callback_number', 'string'),
        'u_aws_scheduled': ('u_aws_scheduled_flag', 'boolean'),
        'u_c3_driver_files': ('u_c3_driver_files', 'string'),
        'u_c3_driver_files_attached': ('u_c3_driver_files_attached_flag', 'boolean'),
        'u_call_reference': ('u_call_reference', 'string'),
        'u_chase_in_contact_count': ('u_chase_in_contact_count', 'integer'),
        'u_chase_out_contact_count': ('u_chase_out_contact_count', 'integer'),
        'u_chat_count': ('u_chat_count', 'integer'),
        'u_customer_escalated': ('u_customer_escalated_flag', 'boolean'),
        'u_daisy_dispatched': ('u_daisy_dispatched', 'string'),
        'u_daisy_offsite': ('u_daisy_offsite', 'string'),
        'u_daisy_onsite': ('u_daisy_onsite', 'string'),
        'u_daisy_resolved': ('u_daisy_resolved', 'string'),
        'u_daisy_ticket': ('u_daisy_ticket', 'string'),
        'u_details_of_the_impact': ('u_details_of_the_impact', 'string'),
        'u_email_count': ('u_email_count', 'integer'),
        'u_first_time_fix': ('u_first_time_fix_flag', 'boolean'),
        'u_go_live': ('u_go_live', 'string'),
        'u_happysignal_feedback_number': ('u_happysignal_feedback_number', 'double'),
        'u_happysignal_score': ('u_happysignal_score', 'double'),
        'u_incident_from_chat': ('u_incident_from_chat_flag', 'boolean'),
        'u_incident_type': ('u_incident_type', 'string'),
        'u_is_this_store_high_value_contactless': ('u_is_this_store_high_value_contactless', 'string'),
        'u_issue_type': ('u_issue_type', 'string'),
        'u_ka_attached': ('u_ka_attached', 'string'),
        'u_major': ('u_major_flag', 'boolean'),
        'u_master': ('u_master_flag', 'boolean'),
        'u_native_ui': ('u_native_ui_flag', 'boolean'),
        'u_notstrikebutton': ('u_notstrikebutton_flag', 'boolean'),
        'u_o2_ticket': ('u_o2_ticket', 'string'),
        'u_phone_count': ('u_phone_count', 'integer'),
        'u_projects': ('u_projects', 'string'),
        'u_remove_engineer_visit': ('u_remove_engineer_visit_flag', 'boolean'),
        'u_resolution_confirmation': ('u_resolution_confirmation', 'string'),
        'u_sdm_escalated': ('u_sdm_escalated_flag', 'boolean'),
        'u_sdm_escalation_time': ('u_sdm_escalation_time', 'string'),
        'u_strike_count_new': ('u_strike_count_new', 'integer'),
        'u_strike_visibility': ('u_strike_visibility_flag', 'boolean'),
        'u_strikebuttonclick': ('u_strikebuttonclick_flag', 'boolean'),
        'u_sub_resolution_code': ('u_sub_resolution_code', 'string'),
        'u_supporting_role': ('u_supporting_role', 'string'),
        'u_survey_triggered': ('u_survey_triggered_flag', 'boolean'),
        'u_techsee_agentlink': ('u_techsee_agentlink', 'string'),
        'u_techsee_assistance_1': ('u_techsee_assistance_1', 'string'),
        'u_techsee_authentication_id': ('u_techsee_authentication_id', 'string'),
        'u_techsee_customerlink': ('u_techsee_customerlink', 'string'),
        'u_techsee_session': ('u_techsee_session', 'string'),
        'u_techsee_session_id_url': ('u_techsee_session_id_url', 'string'),
        'u_techsee_sessionid': ('u_techsee_sessionid', 'string'),
        'u_total_strike_count': ('u_total_strike_count', 'string'),
        'u_trade_impact': ('u_trade_impact', 'string'),
        'u_vendor_not_found': ('u_vendor_not_found_flag', 'boolean'),
        'u_vendor_ticket': ('u_vendor_ticket', 'string'),
        'u_vista_id': ('u_vista_id', 'string'),
        'u_walkin_count': ('u_walkin_count', 'integer'),
        'universal_request': ('universal_request', 'string'),
        'upon_approval': ('upon_approval', 'string'),
        'upon_reject': ('upon_reject', 'string'),
        'urgency': ('urgency', 'string'),
        'user_input': ('user_input', 'string'),
        'watch_list': ('watch_list', 'string'),
        'work_notes': ('work_notes', 'string'),
        'work_notes_list': ('work_notes_list', 'string'),
        'work_start': ('work_start', 'string'),
        'created_year': ('created_year', 'integer'),
        'created_month': ('created_month', 'integer'),
        'u_franchisee_regional_manager_display_value': ('u_franchisee_regional_manager', 'string'),
        'u_franchisee_regional_manager_link': ('u_franchisee_regional_manager_sys_user', 'string'),
        'u_franchisees_consultant_display_value': ('u_franchisees_consultant', 'string'),
        'u_franchisees_consultant_link': ('u_franchisees_consultant_sys_user', 'string'),
        'u_franchisee_director_display_value': ('u_franchisee_director', 'string'),
        'u_franchisee_director_link': ('u_franchisee_director_sys_user', 'string'),
        'cmdb_ci_display_value': ('cmdb_ci', 'string'),
        'cmdb_ci_link': ('cmdb_ci_link', 'string'),
        'u_vendor_display_value': ('u_vendor', 'string'),
        'u_vendor_link': ('u_vendor_sys_user_group', 'string'),
        'u_connect_chat_display_value': ('u_connect_chat', 'string'),
        'u_connect_chat_link': ('u_connect_chat_chat_queue_entry', 'string'),
        'service_offering_display_value': ('service_offering', 'string'),
        'service_offering_link': ('service_offering_link', 'string'),
        'closed_by_display_value': ('closed_by', 'string'),
        'closed_by_link': ('closed_by_sys_user', 'string'),
        'parent_incident_display_value': ('parent_incident_number', 'string'),
        'parent_incident_link': ('parent_incident_link', 'string'),
        'reopened_by_display_value': ('reopened_by', 'string'),
        'reopened_by_link': ('reopened_by_sys_user', 'string'),
        'assigned_to_display_value': ('assigned_to', 'string'),
        'assigned_to_link': ('assigned_to_sys_user', 'string'),
        'u_reopen_detail_display_value': ('u_reopen_detail', 'string'),
        'u_reopen_detail_link': ('u_reopen_detail_link', 'string'),
        'u_chased_out_detail_display_value': ('u_chased_out_detail', 'string'),
        'u_chased_out_detail_link': ('u_chased_out_detail_link', 'string'),
        'u_franchisee_ops_manager_display_value': ('u_franchisee_ops_manager', 'string'),
        'u_franchisee_ops_manager_link': ('u_franchisee_ops_manager_sys_user', 'string'),
        'resolved_by_display_value': ('resolved_by', 'string'),
        'resolved_by_link': ('resolved_by_sys_user', 'string'),
        'opened_by_display_value': ('opened_by', 'string'),
        'opened_by_link': ('opened_by_sys_user', 'string'),
        'sys_domain_display_value': ('sys_domain', 'string'),
        'sys_domain_link': ('sys_domain_link', 'string'),
        'u_franchisee_display_value': ('u_franchisee', 'string'),
        'u_franchisee_link': ('u_franchisee_sys_user', 'string'),
        'u_franchisees_chief_ops_manager_display_value': ('u_franchisees_chief_ops_manager', 'string'),
        'u_franchisees_chief_ops_manager_link': ('u_franchisees_chief_ops_manager_sys_user', 'string'),
        'u_strike_details_display_value': ('u_strike_details', 'string'),
        'u_strike_details_link': ('u_strike_details_link', 'string'),
        'business_service_display_value': ('business_service', 'string'),
        'business_service_link': ('business_service_link', 'string'),
        'caller_id_display_value': ('caller_id', 'string'),
        'caller_id_link': ('caller_id_sys_user', 'string'),
        'u_related_kb_article_display_value': ('u_related_kb_article', 'string'),
        'u_related_kb_article_link': ('u_related_kb_article_link', 'string'),
        'assignment_group_display_value': ('assignment_group', 'string'),
        'assignment_group_link': ('assignment_group_link', 'string'),
        'u_last_assignment_group_display_value': ('u_last_assignment_group', 'string'),
        'u_last_assignment_group_link': ('u_last_assignment_group_link', 'string'),
        'problem_id_display_value': ('problem_id_number', 'string'),
        'problem_id_link': ('problem_id_link', 'string'),
        'company_display_value': ('company', 'string'),
        'company_link': ('company_link', 'string'),
        'u_chased_in_detail_display_value': ('u_chased_in_detail', 'string'),
        'u_chased_in_detail_link': ('u_chased_in_detail_link', 'string'),
        'location_display_value': ('location', 'string'),
        'location_link': ('location_link', 'string'),
        'rfc_display_value': ('rfc', 'string'),
        'rfc_link': ('rfc_link', 'string'),
        'business_duration_seconds': ('business_duration_seconds', 'integer'),
        'u_time_store_may_close_seconds': ('u_time_store_may_close_seconds', 'integer'),
        'u_time_dt_may_close_seconds': ('u_time_dt_may_close_seconds', 'integer'),
        'u_time_store_ok2operate_seconds': ('u_time_store_ok2operate_seconds', 'integer'),
        'u_time_dt_down_seconds': ('u_time_dt_down_seconds', 'integer'),
        'u_time_dt_ok2operate_seconds': ('u_time_dt_ok2operate_seconds', 'integer'),
        'calendar_duration_seconds': ('calendar_duration_seconds', 'integer'),
        'u_time_store_down_seconds': ('u_time_store_down_seconds', 'integer'),
        'sys_updated_on_dt': ('sys_updated_date', 'date'),
        'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
        'u_vista_dispatched_eta_dt': ('u_vista_dispatched_eta_date', 'date'),
        'u_vista_dispatched_eta_timestamp': ('u_vista_dispatched_eta_timestamp', 'string'),
        'u_vista_offsite_dt': ('u_vista_offsite_date', 'date'),
        'u_vista_offsite_timestamp': ('u_vista_offsite_timestamp', 'string'),
        'u_vista_onsite_dt': ('u_vista_onsite_date', 'date'),
        'u_vista_onsite_timestamp': ('u_vista_onsite_timestamp', 'string'),
        'u_call_back_time_dt': ('u_call_back_time_date', 'date'),
        'u_call_back_time_timestamp': ('u_call_back_time_timestamp', 'string'),
        'u_last_assignment_time_dt': ('u_last_assignment_time_date', 'date'),
        'u_last_assignment_time_timestamp': ('u_last_assignment_time_timestamp', 'string'),
        'u_techsee_agent_session_start_time_dt': ('u_techsee_agent_session_start_time_date', 'date'),
        'u_techsee_agent_session_start_time_timestamp': ('u_techsee_agent_session_start_timestamp', 'string'),
        'u_l1_5_assignment_time_dt': ('u_l1_5_assignment_time_date', 'date'),
        'u_l1_5_assignment_time_timestamp': ('u_l1_5_assignment_timestamp', 'string'),
        'u_customer_escalation_time_dt': ('u_customer_escalation_time_date', 'date'),
        'u_customer_escalation_time_timestamp': ('u_customer_escalation_timestamp', 'string'),
        'sys_created_on_dt': ('sys_created_date', 'date'),
        'sys_created_on_timestamp': ('sys_created_timestamp', 'string'),
        'closed_at_dt': ('closed_date', 'date'),
        'closed_at_timestamp': ('closed_timestamp', 'string'),
        'opened_at_dt': ('opened_date', 'date'),
        'opened_at_timestamp': ('opened_timestamp', 'string'),
        'work_end_dt': ('work_end_date', 'date'),
        'work_end_timestamp': ('work_end_timestamp', 'string'),
        'reopened_time_dt': ('reopened_time_date', 'date'),
        'reopened_time_timestamp': ('reopened_timestamp', 'string'),
        'resolved_at_dt': ('resolved_at_date', 'date'),
        'resolved_at_timestamp': ('resolved_at_timestamp', 'string'),
        'u_reopen_date_time_dt': ('u_reopen_date_time_date', 'date'),
        'u_reopen_date_time_timestamp': ('u_reopen_date_timestamp', 'string'),
        'restaurant_name': ('restaurant_name', 'string'),
        'restaurant_id': ('restaurant_id', 'integer')                    
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
                self.aws_instance.create_athena_delta_table('processed', 'service_now_service_offering', save_output_path, self.athena_output_path)
                
            else:

                # Merge data to the Delta table
                merge_columns = ['incident_number','sys_created_timestamp','state','created_year','created_month']
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