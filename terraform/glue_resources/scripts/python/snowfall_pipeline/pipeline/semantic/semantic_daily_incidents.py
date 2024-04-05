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

        SELECT
            restaurant_id,
            restaurant_name,
            CASE
                WHEN restaurant_id = -1 THEN restaurant_name 
                ELSE concat(cast(restaurant_id as string), ' ', restaurant_name) 
            END as restaurant_full_name,
            incident_number as incident_id,
            short_description as incident_short_description,
            incident_state as incident_state,
            CASE
                WHEN opened_date = resolved_at_date THEN 'New and Resolved' 
                ELSE incident_state 
            END as eod_incident_status,
            opened_date as opened_at_date,
            concat(cast(opened_date as string), ' ', opened_time) as opened_at_timestamp,
            resolved_at_date as resolved_at_date,
            concat(cast(resolved_at_date as string), ' ', resolved_at_time) as resolved_at_timestamp,
            priority as incident_priority_local,
            priority as incident_priority_global,
            category as incident_category,
            subcategory as incident_subcategory,
            assignment_group,
            service_offering,
            u_vendor as service_vendor,
            updated_date as reporting_date,
            updated_date as last_updated_at_timestamp
        FROM service_now_incident_daily
        WHERE updated_date = '{self.formatted_reporting_date}'

        UNION

        (SELECT 
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
            last_updated_at_timestamp
        FROM (
            SELECT
                restaurant_id,
                restaurant_name,
                CASE
                    WHEN restaurant_id = -1 THEN restaurant_name 
                    ELSE concat(cast(restaurant_id as string), ' ', restaurant_name) 
                END as restaurant_full_name,
                incident_number as incident_id,
                short_description as incident_short_description,
                incident_state as incident_state,
                CASE 
                    WHEN opened_date = resolved_at_date THEN 'New and Resolved' 
                    ELSE state 
                END as eod_incident_status,
                opened_date as opened_at_date,
                concat(cast(opened_date as string), ' ', opened_time) as opened_at_timestamp,
                resolved_at_date as resolved_at_date,
                concat(cast(resolved_at_date as string), ' ', resolved_at_time) as resolved_at_timestamp,
                priority as incident_priority_local,
                priority as incident_priority_global,
                category as incident_category,
                subcategory as incident_subcategory,
                assignment_group,
                service_offering,
                u_vendor as service_vendor,
                '{self.formatted_reporting_date}' as reporting_date,
                concat(cast(updated_date as string), ' ', updated_time) as last_updated_at_timestamp,
                rank() over (partition by incident_number order by cast(concat(cast(updated_date as string), ' ', updated_time) as timestamp) desc) as rank
            FROM service_now_incident_daily
            WHERE updated_date < '{self.formatted_reporting_date}'
            AND incident_number NOT IN (
                    SELECT distinct incident_number 
                    FROM service_now_incident_daily
                    WHERE updated_date <= '{self.formatted_reporting_date}'
                    AND incident_state IN ('Closed', 'Cancelled', 'Duplicate')
            )
            AND incident_state NOT IN ('Closed', 'Cancelled', 'Duplicate')
        ) a
        WHERE a.rank = 1)
        """
        self.logger.info(f"Running the SQL Query: {sql_query}")

        df.createOrReplaceTempView("service_now_incident_daily")

        result_df = self.spark(sql_query)

        column_mapping = {

            'opened_at_timestamp': ('opened_at_timestamp', 'time'),
            'resolved_at_timestamp': ('resolved_at_timestamp', 'time'),
            'reporting_date': ('reporting_date', 'date'),
            'last_updated_at_timestamp':('last_updated_at_timestamp','time')
        }
        
        result_df = self.change_column_names_and_schema(result_df,column_mapping)
        
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
        if DeltaTable.isDeltaTable(self.spark,save_output_path) is False:
            self.athena_trigger = True
            
        # Determine whether to create or merge to the Delta table
        if self.athena_trigger:
            # Create the Delta table
            df.write.format("delta").mode("overwrite").save(save_output_path)

            # Execute Athena query to create the table
            self.aws_instance.create_athena_delta_table('semantic', 'daily_incidents', save_output_path, self.athena_output_path)
            
        else:

            # Load the Delta table as a DeltaTable
            delta_table = DeltaTable.forPath(self.spark,save_output_path)

            # Delete rows with the specified reporting_date
            delta_table.delete(f"reporting_date = '{self.formatted_reporting_date}'")

            # Append the new DataFrame to the Delta table
            df.write.format("delta").mode("append").save(save_output_path)

            # Vaccum the Delta table
            delta_table.vacuum(retentionHours=200)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')
