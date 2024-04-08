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


        # Step 6: Selecting Columns that I want to take to processed layer
        df = df.select(
            'number',
            'sys_created_on_dt',
            'sys_created_on_timestamp',
            'sys_updated_on_dt',
            'sys_updated_on_timestamp',
            'aliases',
            'asset',
            'asset_tag',
            'assigned',
            'assigned_to',
            'assignment_group',
            'attestation_score',
            'attestation_status',
            'attested',
            'attested_by',
            'attested_date',
            'attributes',
            'billing',
            'busines_criticality',
            'business_contact',
            'business_need',
            'business_relation_manager',
            'business_unit',
            'can_print',
            'category',
            'change_control',
            'checked_in',
            'checked_out',
            'checkout',
            'comments',
            'company',
            'compatibility_dependencies',
            'consumer_type',
            'contract',
            'correlation_id',
            'cost',
            'cost_cc',
            'cost_center',
            'delivery_date',
            'delivery_manager',
            'department',
            'description',
            'discovery_source',
            'dns_domain',
            'due',
            'due_in',
            'end_date',
            'environment',
            'fault_count',
            'first_discovered',
            'fqdn',
            'gl_account',
            'install_date',
            'install_status',
            'invoice_number',
            'ip_address',
            'justification',
            'last_discovered',
            'last_review_date',
            'lease_id',
            'life_cycle_stage',
            'life_cycle_stage_status',
            'location',
            'mac_address',
            'maintenance_schedule',
            'managed_by_group',
            'manufacturer',
            'model_id',
            'model_number',
            'monitor',
            'monitoring_requirements',
            'name',
            'operational_status',
            'order',
            'order_date',
            'po_number',
            'portfolio_status',
            'prerequisites',
            'price',
            'price_model',
            'price_unit',
            'published_ref',
            'purchase_date',
            'schedule',
            'serial_number',
            'service_classification',
            'service_level_requirement',
            'service_owner_delegate',
            'service_status',
            'short_description',
            'skip_sync',
            'sla',
            'stakeholders',
            'start_date',
            'state',
            'subcategory',
            'support_group',
            'supported_by',
            'sys_class_name',
            'sys_class_path',
            'sys_created_by',
            'sys_domain_path',
            'sys_id',
            'sys_mod_count',
            'sys_tags',
            'sys_updated_by',
            'technical_contact',
            'u_mcd_operating_system',
            'u_mcd_service_level_pack',
            'u_service_offering',
            'unit_description',
            'unverified',
            'used_for',
            'user_group',
            'vendor',
            'version',
            'warranty_expiration',
            'created_year',
            'created_month',
            'parent_display_value',
            'parent_link',
            'owned_by_display_value',
            'owned_by_link',
            'managed_by_display_value',
            'managed_by_link',
            'sys_domain_display_value',
            'sys_domain_link',
            'duplicate_of_display_value',
            'duplicate_of_link',
        )

        column_mapping = {
            'aliases': ('aliases', 'string'),
            'asset': ('asset', 'string'),
            'asset_tag': ('asset_tag', 'string'),
            'assigned': ('assigned', 'string'),
            'assigned_to': ('assigned_to', 'string'),
            'assignment_group': ('assignment_group', 'string'),
            'attestation_score': ('attestation_score', 'int'),
            'attestation_status': ('attestation_status', 'string'),
            'attested': ('attested', 'boolean'),
            'attested_by': ('attested_by', 'string'),
            'attested_date': ('attested_date', 'date'),
            'attributes': ('attributes', 'string'),
            'billing': ('billing', 'string'),
            'busines_criticality': ('busines_criticality', 'string'),
            'business_contact': ('business_contact', 'string'),
            'business_need': ('business_need', 'string'),
            'business_relation_manager': ('business_relation_manager', 'string'),
            'business_unit': ('business_unit', 'string'),
            'can_print': ('can_print', 'boolean'),
            'category': ('category', 'string'),
            'change_control': ('change_control', 'string'),
            'checked_in': ('checked_in', 'string'),
            'checked_out': ('checked_out', 'string'),
            'checkout': ('checkout', 'string'),
            'comments': ('comments', 'string'),
            'company': ('company', 'string'),
            'compatibility_dependencies': ('compatibility_dependencies', 'string'),
            'consumer_type': ('consumer_type', 'string'),
            'contract': ('contract', 'string'),
            'correlation_id': ('correlation_id', 'string'),
            'cost': ('cost', 'string'),
            'cost_cc': ('cost_cc', 'string'),
            'cost_center': ('cost_center', 'string'),
            'delivery_date': ('delivery_date', 'date'),
            'delivery_manager': ('delivery_manager', 'string'),
            'department': ('department', 'string'),
            'description': ('description', 'string'),
            'discovery_source': ('discovery_source', 'string'),
            'dns_domain': ('dns_domain', 'string'),
            'due': ('due', 'string'),
            'due_in': ('due_in', 'string'),
            'end_date': ('end_date', 'date'),
            'environment': ('environment', 'string'),
            'fault_count': ('fault_count', 'int'),
            'first_discovered': ('first_discovered', 'string'),
            'fqdn': ('fqdn', 'string'),
            'gl_account': ('gl_account', 'string'),
            'install_date': ('install_date', 'date'),
            'install_status': ('install_status', 'string'),
            'invoice_number': ('invoice_number', 'string'),
            'ip_address': ('ip_address', 'string'),
            'justification': ('justification', 'string'),
            'last_discovered': ('last_discovered', 'string'),
            'last_review_date': ('last_review_date', 'date'),
            'lease_id': ('lease_id', 'string'),
            'life_cycle_stage': ('life_cycle_stage', 'string'),
            'life_cycle_stage_status': ('life_cycle_stage_status', 'string'),
            'location': ('location', 'string'),
            'mac_address': ('mac_address', 'string'),
            'maintenance_schedule': ('maintenance_schedule', 'string'),
            'managed_by_group': ('managed_by_group', 'string'),
            'manufacturer': ('manufacturer', 'string'),
            'model_id': ('model_id', 'string'),
            'model_number': ('model_number', 'string'),
            'monitor': ('monitor', 'string'),
            'monitoring_requirements': ('monitoring_requirements', 'string'),
            'name': ('name', 'string'),
            'number': ('service_number', 'string'),
            'operational_status': ('operational_status', 'string'),
            'order': ('order', 'string'),
            'order_date': ('order_date', 'date'),
            'po_number': ('po_number', 'string'),
            'portfolio_status': ('portfolio_status', 'string'),
            'prerequisites': ('prerequisites', 'string'),
            'price': ('price', 'string'),
            'price_model': ('price_model', 'string'),
            'price_unit': ('price_unit', 'string'),
            'published_ref': ('published_ref', 'string'),
            'purchase_date': ('purchase_date', 'date'),
            'schedule': ('schedule', 'string'),
            'serial_number': ('serial_number', 'string'),
            'service_classification': ('service_classification', 'string'),
            'service_level_requirement': ('service_level_requirement', 'string'),
            'service_owner_delegate': ('service_owner_delegate', 'string'),
            'service_status': ('service_status', 'string'),
            'short_description': ('short_description', 'string'),
            'skip_sync': ('skip_sync', 'boolean'),
            'sla': ('sla', 'string'),
            'stakeholders': ('stakeholders', 'string'),
            'start_date': ('start_date', 'date'),
            'state': ('state', 'string'),
            'subcategory': ('subcategory', 'string'),
            'support_group': ('support_group', 'string'),
            'supported_by': ('supported_by', 'string'),
            'sys_class_name': ('sys_class_name', 'string'),
            'sys_class_path': ('sys_class_path', 'string'),
            'sys_created_by': ('sys_created_by', 'string'),
            'sys_domain_path': ('sys_domain_path', 'string'),
            'sys_id': ('sys_id', 'string'),
            'sys_mod_count': ('sys_mod_count', 'integer'),
            'sys_tags': ('sys_tags', 'string'),
            'sys_updated_by': ('sys_updated_by', 'string'),
            'technical_contact': ('technical_contact', 'string'),
            'u_mcd_operating_system': ('u_mcd_operating_system', 'string'),
            'u_mcd_service_level_pack': ('u_mcd_service_level_pack', 'string'),
            'u_service_offering': ('u_service_offering', 'string'),
            'unit_description': ('unit_description', 'string'),
            'unverified': ('unverified', 'string'),
            'used_for': ('used_for', 'string'),
            'user_group': ('user_group', 'string'),
            'vendor': ('vendor', 'string'),
            'version': ('version', 'string'),
            'warranty_expiration': ('warranty_expiration', 'date'),
            'created_year': ('created_year', 'int'),
            'created_month': ('created_month', 'int'),
            'parent_display_value': ('parent', 'string'),
            'parent_link': ('parent_cmdb_link', 'string'),
            'owned_by_display_value': ('owned_by', 'string'),
            'owned_by_link': ('owned_by_sys_user_link', 'string'),
            'managed_by_display_value': ('managed_by', 'string'),
            'managed_by_link': ('managed_by_sys_user_link', 'string'),
            'sys_domain_display_value': ('sys_domain', 'string'),
            'sys_domain_link': ('sys_domain_sys_user_link', 'string'),
            'duplicate_of_display_value': ('duplicate_of', 'string'),
            'duplicate_of_link': ('duplicate_of_cmdb_link', 'string'),
            'sys_updated_on_dt': ('sys_updated_date', 'date'),
            'sys_updated_on_timestamp': ('sys_updated_timestamp', 'string'),
            'sys_created_on_dt': ('sys_created_date', 'date'),
            'sys_created_on_timestamp': ('sys_created_timestamp', 'string')
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
                merge_columns = ['service_number','sys_created_timestamp','created_year','created_month']
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