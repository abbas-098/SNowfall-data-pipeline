from snowfall_pipeline.common_utilities.transform_base import TransformBase
from datetime import datetime, timedelta
from delta.tables import DeltaTable

class SemanticDailyIncidents(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.max_date = self.aws_instance.get_workflow_properties('MAX_DATE')


    def get_data(self):

        # Check if max_date is empty
        if self.max_date:
            try:
                # Parse max_date if not empty
                max_date_obj = datetime.strptime(self.max_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError("Invalid date format. Please provide date in 'yyyy-mm-dd' format.")
        else:
            # If max_date is empty, get yesterday's date
            max_date_obj = datetime.now() - timedelta(days=1)

        # Format max_date to 'yyyy-mm-dd' format
        formatted_max_date = max_date_obj.strftime('%Y-%m-%d')

        self.logger.info(formatted_max_date)

        sql_query = f"""
                SELECT
                    restaurant_id,
                    restaurant_name,
                    concat(Cast(restaurant_id as varchar),' ',restaurant_name) AS restaurant_full_name,
                    incident_number,
                    short_description,
                    state,
                    incident_state,
                    opened_date,
                    opened_time,
                    resolved_at_date,
                    resolved_at_time,
                    priority,
                    priority,
                    category,
                    subcategory,
                    assignment_group,
                    service_offering,
                    u_vendor,
                    updated_date
                FROM uk_snowfall_processed.service_now_incident_daily
                Where updated_date = cast('{formatted_max_date}' as date)
            """
        self.logger.info(sql_query)
        df = self.aws_instance.athena_query_to_df('uk_snowfall_processed',sql_query)
        self.logger.info(df)
        return df



    def transform_data(self, df):
        return df



    def save_data(self, df):
        """
        Save DataFrame to an S3 location and create/update a Delta table if needed.

        Parameters:
        - df (DataFrame): Input DataFrame to be saved.

        """
        return df
