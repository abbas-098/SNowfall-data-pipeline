from snowfall_pipeline.common_utilities.transform_base import TransformBase
from datetime import datetime, timedelta
from delta.tables import DeltaTable


class SemanticDailyIncidents(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.reporting_date = self.aws_instance.get_workflow_properties('REPORTING_DATE')
        self.file_path = "service_now/incident/daily"


    def get_data(self):
        df = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/{self.file_path}/")
        return df

    def transform_data(self, df):

        df.createOrReplaceTempView("service_now_incident_daily")

        # Check if max_date is empty
        if self.reporting_date:
            try:
                # Parse max_date if not empty
                report_date_obj = datetime.strptime(self.reporting_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError("Invalid date format. Please provide date in 'yyyy-mm-dd' format.")
        else:
            # If max_date is empty, get yesterday's date
            report_date_obj = datetime.now() - timedelta(days=1)

        # Format max_date to 'yyyy-mm-dd' format
        self.formatted_reporting_date = report_date_obj.strftime('%Y-%m-%d')

        self.logger.info(f"Reporting date selected: {self.formatted_reporting_date}")

        sql_query = f"""
            with incidents_prep as (
                select restaurant_id
                     , restaurant_name
                     , case when restaurant_id = -1 then restaurant_name
                            else concat(cast(restaurant_id as string),' ',restaurant_name) 
                            end as restaurant_full_name
                     , incident_number                                                          as incident_id
                     , short_description                                                        as incident_short_description
                     , incident_state                                                           as incident_state
                     , case when opened_date = resolved_at_date and opened_date = date('{self.formatted_reporting_date}') 
                                        then 'New and Resolved' else state end                  as eod_incident_status
                     , sys_updated_date                                                         as sys_updated_date
                     , sys_updated_timestamp                                                    as sys_updated_timestamp
                     , opened_date                                                              as opened_at_date
                     , opened_timestamp                                                         as opened_at_timestamp
                     , resolved_at_date                                                         as resolved_at_date
                     , resolved_at_timestamp                                                    as resolved_at_timestamp
                     , priority                                                                 as incident_priority_local
                     , priority                                                                 as incident_priority_global
                     , category                                                                 as incident_category
                     , subcategory                                                              as incident_subcategory
                     , assignment_group                                                         as assignment_group
                     , service_offering                                                         as service_offering
                     , u_vendor                                                                 as service_vendor
                     , case when incident_state in ('Closed','Cancelled','Duplicate') then 1 else 0 end     as incident_resolved_flag
                     , case when sys_updated_date = date('{self.formatted_reporting_date}') then 1 else 0 end    as latest_record_flag
                     , date('{self.formatted_reporting_date}')                                        as reporting_date
                from service_now_incident_daily   
                where sys_updated_date <= date('{self.formatted_reporting_date}')
            ), incidents as (
                select *
                     , case when incident_resolved_flag = 1 and latest_record_flag = 1 then 1 else 0 end    as incident_resolved_filter_flag
                     , case when incident_resolved_flag = 0 then 1 else 0 end     as incident_unresolved_filter_flag
                     , rank() over (partition by incident_id order by incident_resolved_flag desc, sys_updated_timestamp desc) as incident_rank
                from incidents_prep
            )
            select
                restaurant_id,
                restaurant_name,
                restaurant_full_name,
                incident_id,
                incident_short_description,
                incident_state,
                eod_incident_status,
                opened_at_date,
                opened_at_timestamp,
                resolved_at_date,
                resolved_at_timestamp,
                incident_priority_local,
                incident_priority_global,
                incident_category,
                incident_subcategory,
                assignment_group,
                service_offering,
                service_vendor,
                reporting_date,
                sys_updated_date from incidents
            where incident_rank = 1
            and (incident_resolved_filter_flag = 1 or incident_unresolved_filter_flag = 1)
        """
        self.logger.info(f"Running the SQL Query: {sql_query}")

        result_df = self.spark.sql(sql_query)

        return result_df

    def save_data(self, df):
        """
        Save DataFrame to an S3 location and create/update a Delta table if needed.

        Parameters:
        - df (DataFrame): Input DataFrame to be saved.

        """
        # Define the S3 save path
        save_output_path = f"s3://{self.semantic_bucket_name}/daily_incidents/"

        # Check if Delta table needs to be created
        if DeltaTable.isDeltaTable(self.spark, save_output_path) is False:
            self.athena_trigger = True

        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:
            # Create the Delta table
            df.write.format("delta").mode("overwrite") \
                .partitionBy('reporting_date') \
                .save(save_output_path)

            # Execute Athena query to create the table
            execution_query_id = self.aws_instance.create_athena_delta_table('semantic', 'view_daily_incident_snapshot',
                                                                             save_output_path,
                                                                             self.athena_output_path)
            # Change string data type to timestamp via glue schema
            if self.aws_instance.check_query_status(execution_query_id) is True:
                timestamp_columns = [
                    'opened_at_timestamp',
                    'resolved_at_timestamp'
                ]
                self.aws_instance.update_table_columns_to_timestamp('semantic', 'view_daily_incident_snapshot',
                                                                    timestamp_columns)

        else:

            # Load the Delta table as a DeltaTable
            delta_table = DeltaTable.forPath(self.spark, save_output_path)

            # Delete rows with the specified reporting_date
            delta_table.delete(f"reporting_date = '{self.formatted_reporting_date}'")

            # Append the new DataFrame to the Delta table
            df.write.format("delta").mode("append") \
                .partitionBy('reporting_date') \
                .save(save_output_path)

            # Vaccum the Delta table
            delta_table.vacuum(retentionHours=200)

        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')
