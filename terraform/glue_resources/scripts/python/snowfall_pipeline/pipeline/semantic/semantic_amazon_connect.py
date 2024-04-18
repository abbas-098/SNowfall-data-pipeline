from snowfall_pipeline.common_utilities.transform_base import TransformBase
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from pyspark.sql.types import LongType


class SemanticAmazonConnect(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.reporting_date = self.aws_instance.get_workflow_properties('REPORTING_DATE')
        self.file_path = "amazon_connect"

    def get_data(self):
        df = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/{self.file_path}/")
        return df

    def transform_data(self, df):

        df.createOrReplaceTempView("amazon_connect")

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
                        file_upload_date AS reporting_date,
                        Queue AS call_queue,
                        contacts_answered_in_15_seconds AS calls_handled_within_15_seconds,
                        contacts_answered_in_30_seconds AS calls_handled_within_30_seconds,
                        contacts_answered_in_45_seconds AS calls_handled_within_45_seconds,
                        contacts_handled_incoming AS calls_handled,
                        contacts_incoming AS calls_offered,
                        contacts_abandoned - contacts_abandoned_in_45_seconds AS calls_abandoned,
                        CAST(average_queue_answer_time * contacts_handled_incoming AS BIGINT) AS total_customer_duration_seconds,
                        CAST(average_handle_time * contacts_handled_incoming AS BIGINT) AS total_call_duration_seconds,
                        contacts_answered_in_45_seconds / (contacts_incoming - contacts_abandoned_in_45_seconds) AS grade_of_service
                    FROM amazon_connect
                    WHERE file_upload_date = '{self.formatted_reporting_date}'
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
        save_output_path = f"s3://{self.semantic_bucket_name}/amazon_connect/"

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
            self.aws_instance.create_athena_delta_table('semantic', 'view_daily_amazon_connect_snapshot', save_output_path,
                                                        self.athena_output_path)

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
