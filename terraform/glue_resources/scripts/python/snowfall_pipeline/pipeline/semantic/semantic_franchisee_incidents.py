from snowfall_pipeline.common_utilities.transform_base import TransformBase
from datetime import datetime, timedelta
from delta.tables import DeltaTable


class SemanticFranchiseeIncidents(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.reporting_date = self.aws_instance.get_workflow_properties('REPORTING_DATE')
        self.file_path = "service_now/incident/daily"
        self.file_path2 = "service_now/location"
        self.file_path3 = "ods/location_hierarchy"

    def get_data(self):
        df = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/{self.file_path}/")
        df2 = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/{self.file_path2}/")
        df3 = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/{self.file_path3}/")
        return df, df2, df3

    def process_flow(self):
        "Abstract method which runs each pipeline in order"
        try:
            df, df2, df3 = self.get_data()
            transformed_df = self.transform_data(df, df2, df3)
            self.save_data(transformed_df)
        except Exception as e:
            if 'Preparation' in self.__class__.__name__:
                for i in self.list_of_files:
                    self.aws_instance.move_s3_object(self.raw_bucket_name, i, f"error/{i}")
                self.aws_instance.send_sns_message(e)
                raise e
            else:
                self.aws_instance.send_sns_message(e)
                raise e

    def transform_data(self, df, df2, df3):

        df.createOrReplaceTempView("service_now_incident_daily")
        df2.createOrReplaceTempView("service_now_location")
        df3.createOrReplaceTempView("ods_location_hierarchy")

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
            WITH RankedIncidents AS (
                SELECT
                    incident.restaurant_id AS restaurant_id,
                    incident.restaurant_name AS restaurant_name,
                    CASE 
                        WHEN incident.restaurant_id = -1 THEN incident.restaurant_name 
                        ELSE CONCAT(CAST(incident.restaurant_id AS string), ' ', incident.restaurant_name) 
                    END AS restaurant_full_name,
                    incident.incident_number AS incident_id,
                    incident.short_description AS incident_short_description,
                    incident.incident_state AS incident_state,
                    CASE 
                        WHEN incident.opened_date = incident.resolved_at_date THEN 'New and Resolved' 
                        ELSE incident.state 
                    END AS eod_incident_status,
                    incident.opened_date AS opened_at_date,
                    incident.opened_timestamp AS opened_at_timestamp,
                    incident.resolved_at_date AS resolved_at_date,
                    incident.resolved_at_timestamp AS resolved_at_timestamp,
                    incident.calendar_stc AS business_duration_seconds,
                    incident.priority AS incident_priority_local,
                    incident.priority AS incident_priority_global,
                    incident.category AS incident_category,
                    incident.subcategory AS incident_subcategory,
                    incident.assignment_group AS assignment_group,
                    incident.service_offering AS service_offering,
                    incident.u_vendor AS service_vendor,
                    incident.u_vendor_ticket AS service_vendor_ticket_id,
                    incident.u_happysignal_feedback_number AS happy_signal_feedback_id,
                    incident.u_happysignal_score AS happy_signal_score,
                    incident.u_techsee_session AS techsee_session_id,
                    CASE 
                        WHEN LENGTH(incident.u_techsee_sessionid) > 0 THEN true 
                        ELSE false 
                    END AS techsee_flag,
                    incident.u_vista_id AS vista_id,
                    CASE 
                        WHEN LENGTH(incident.u_vista_id) > 0 THEN true
                        ELSE false 
                    END AS vista_flag,
                    CASE 
                        WHEN incident.u_vendor = 'McD-UK Vista' THEN true 
                        WHEN incident.u_vendor = 'McD UK - Partner - Acrelec' THEN true 
                        WHEN incident.u_vendor = 'McD UK - Partner - Evoke' THEN true 
                        WHEN incident.u_vendor = 'McD UK - Partner - Odema' THEN true
                        ELSE false 
                    END AS engineer_flag,
                    location_hierarchy.oo_full_name AS franchisee,
                    location_hierarchy.oo_email AS franchisee_email,
                    location_hierarchy.oo_eid AS franchisee_id,
                    location_hierarchy.oo_hierarchy_no AS franchisee_hierarchy_id,
                    incident.u_franchisee_regional_manager AS franchisee_manager,
                    location.location_type AS restaurant_type,
                    location.u_drive_thru_flag AS drive_thru_flag,
                    location.country AS country,
                    location.u_rlg1 AS region,
                    location.city AS city,
                    location_hierarchy.postcode AS postcode,
                    location_hierarchy.longitude AS longitude,
                    location_hierarchy.latitude AS latitude,
                    incident.sys_updated_date as reporting_date,
                    rank() over (partition by incident.incident_number order by cast(incident.sys_updated_timestamp as timestamp) desc) 
                    as incident_rank
                FROM 
                    service_now_incident_daily as incident
                INNER JOIN service_now_location as location
                    ON incident.restaurant_id = location.restaurant_id
                INNER JOIN ods_location_hierarchy AS location_hierarchy
                    ON incident.restaurant_id = location_hierarchy.store_number
                WHERE incident.sys_updated_date = '{self.formatted_reporting_date}' 
            )
            SELECT *
            FROM RankedIncidents
            WHERE incident_rank = 1;
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
        save_output_path = f"s3://{self.semantic_bucket_name}/franchisee_incidents/"

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
            execution_query_id = self.aws_instance.create_athena_delta_table('semantic',
                                                                             'view_daily_franchisee_restaurant_incidents',
                                                                             save_output_path,
                                                                             self.athena_output_path)
            # Change string data type to timestamp via glue schema
            if self.aws_instance.check_query_status(execution_query_id) is True:
                timestamp_columns = [
                    'opened_at_timestamp',
                    'resolved_at_timestamp'
                ]
                self.aws_instance.update_table_columns_to_timestamp('semantic',
                                                                    'view_daily_franchisee_restaurant_incidents',
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
