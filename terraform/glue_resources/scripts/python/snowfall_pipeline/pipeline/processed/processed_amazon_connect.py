from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable

class ProcessedAmazonConnect(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "amazon_connect"


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
            'queue': ('queue', 'string'),
            'agent_answer_rate': ('agent_answer_rate', 'string'),
            'agent_api_connecting_time': ('agent_api_connecting_time', 'integer'),
            'agent_callback_connecting_time': ('agent_callback_connecting_time', 'integer'),
            'agent_idle_time': ('agent_idle_time', 'integer'),
            'agent_incoming_connecting_time': ('agent_incoming_connecting_time', 'integer'),
            'contacts_missed': ('contacts_missed', 'integer'),
            'agent_on_contact_time': ('agent_on_contact_time', 'integer'),
            'agent_outbound_connecting_time': ('agent_outbound_connecting_time', 'integer'),
            'average_agent_api_connecting_time': ('average_agent_api_connecting_time', 'integer'),
            'average_agent_callback_connecting_time': ('average_agent_callback_connecting_time', 'integer'),
            'average_agent_incoming_connecting_time': ('average_agent_incoming_connecting_time', 'integer'),
            'average_agent_outbound_connecting_time': ('average_agent_outbound_connecting_time', 'integer'),
            'nonproductive_time': ('nonproductive_time', 'integer'),
            'occupancy': ('occupancy', 'string'),
            'online_time': ('online_time', 'integer'),
            'adherence': ('adherence', 'integer'),
            'adherent_time': ('adherent_time', 'integer'),
            'non-adherent_time': ('non-adherent_time', 'integer'),
            'scheduled_time': ('scheduled_time', 'integer'),
            'contacts_abandoned_in_15_seconds': ('contacts_abandoned_in_15_seconds', 'integer'),
            'contacts_abandoned_in_20_seconds': ('contacts_abandoned_in_20_seconds', 'integer'),
            'contacts_abandoned_in_25_seconds': ('contacts_abandoned_in_25_seconds', 'integer'),
            'contacts_abandoned_in_30_seconds': ('contacts_abandoned_in_30_seconds', 'integer'),
            'contacts_abandoned_in_45_seconds': ('contacts_abandoned_in_45_seconds', 'integer'),
            'contacts_abandoned_in_60_seconds': ('contacts_abandoned_in_60_seconds', 'integer'),
            'contacts_abandoned_in_90_seconds': ('contacts_abandoned_in_90_seconds', 'integer'),
            'contacts_abandoned_in_120_seconds': ('contacts_abandoned_in_120_seconds', 'integer'),
            'contacts_abandoned_in_180_seconds': ('contacts_abandoned_in_180_seconds', 'integer'),
            'contacts_abandoned_in_240_seconds': ('contacts_abandoned_in_240_seconds', 'integer'),
            'contacts_answered_in_15_seconds': ('contacts_answered_in_15_seconds', 'integer'),
            'contacts_answered_in_20_seconds': ('contacts_answered_in_20_seconds', 'integer'),
            'contacts_answered_in_25_seconds': ('contacts_answered_in_25_seconds', 'integer'),
            'contacts_answered_in_30_seconds': ('contacts_answered_in_30_seconds', 'integer'),
            'contacts_answered_in_45_seconds': ('contacts_answered_in_45_seconds', 'integer'),
            'contacts_answered_in_60_seconds': ('contacts_answered_in_60_seconds', 'integer'),
            'contacts_answered_in_90_seconds': ('contacts_answered_in_90_seconds', 'integer'),
            'contacts_answered_in_120_seconds': ('contacts_answered_in_120_seconds', 'integer'),
            'service_level_15_seconds': ('service_level_15_seconds', 'string'),
            'service_level_20_seconds': ('service_level_20_seconds', 'string'),
            'service_level_25_seconds': ('service_level_25_seconds', 'string'),
            'service_level_30_seconds': ('service_level_30_seconds', 'string'),
            'service_level_45_seconds': ('service_level_45_seconds', 'string'),
            'service_level_60_seconds': ('service_level_60_seconds', 'string'),
            'service_level_90_seconds': ('service_level_90_seconds', 'string'),
            'service_level_120_seconds': ('service_level_120_seconds', 'string'),
            'after_contact_work_time': ('after_contact_work_time', 'integer'),
            'agent_interaction_and_hold_time': ('agent_interaction_and_hold_time', 'integer'),
            'agent_interaction_time': ('agent_interaction_time', 'integer'),
            'api_contacts': ('api_contacts', 'integer'),
            'api_contacts_handled': ('api_contacts_handled', 'integer'),
            'average_after_contact_work_time': ('average_after_contact_work_time', 'integer'),
            'average_agent_interaction_and_customer_hold_time': ('average_agent_interaction_and_customer_hold_time', 'integer'),
            'average_agent_interaction_time': ('average_agent_interaction_time', 'integer'),
            'average_customer_hold_time': ('average_customer_hold_time', 'integer'),
            'average_handle_time': ('average_handle_time', 'integer'),
            'average_outbound_after_contact_work_time': ('average_outbound_after_contact_work_time', 'integer'),
            'average_outbound_agent_interaction_time': ('average_outbound_agent_interaction_time', 'integer'),
            'average_queue_abandon_time': ('average_queue_abandon_time', 'integer'),
            'average_queue_answer_time': ('average_queue_answer_time', 'integer'),
            'callback_contacts': ('callback_contacts', 'integer'),
            'callback_contacts_handled': ('callback_contacts_handled', 'integer'),
            'contact_flow_time': ('contact_flow_time', 'integer'),
            'contact_handle_time': ('contact_handle_time', 'integer'),
            'contacts_abandoned': ('contacts_abandoned', 'integer'),
            'contacts_agent_hung_up_first': ('contacts_agent_hung_up_first', 'integer'),
            'contacts_handled': ('contacts_handled', 'integer'),
            'contacts_handled_incoming': ('contacts_handled_incoming', 'integer'),
            'contacts_handled_outbound': ('contacts_handled_outbound', 'integer'),
            'contacts_hold_agent_disconnect': ('contacts_hold_agent_disconnect', 'integer'),
            'contacts_hold_customer_disconnect': ('contacts_hold_customer_disconnect', 'integer'),
            'contacts_hold_disconnect': ('contacts_hold_disconnect', 'integer'),
            'contacts_incoming': ('contacts_incoming', 'integer'),
            'contacts_put_on_hold': ('contacts_put_on_hold', 'integer'),
            'contacts_queued': ('contacts_queued', 'integer'),
            'contacts_transferred_in': ('contacts_transferred_in', 'integer'),
            'contacts_transferred_in_by_agent': ('contacts_transferred_in_by_agent', 'integer'),
            'contacts_transferred_in_from_queue': ('contacts_transferred_in_from_queue', 'integer'),
            'contacts_transferred_out': ('contacts_transferred_out', 'integer'),
            'contacts_transferred_out_external': ('contacts_transferred_out_external', 'integer'),
            'contacts_transferred_out_by_agent': ('contacts_transferred_out_by_agent', 'integer'),
            'contacts_transferred_out_from_queue': ('contacts_transferred_out_from_queue', 'integer'),
            'contacts_transferred_out_internal': ('contacts_transferred_out_internal', 'integer'),
            'customer_hold_time': ('customer_hold_time', 'integer'),
            'maximum_queued_time': ('maximum_queued_time', 'integer'),
            'file_upload_date' : ('file_upload_date', 'date'),
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
            self.aws_instance.create_athena_delta_table('processed', 'amazon_connect', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['queue','file_upload_date']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')



