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
        location_df = self.spark.read.format("delta").load(f"s3://{self.processed_bucket_name}/service_now/location/")
        df = self.process_change_request(df,'u_site',location_df)

        # Step 5: Joining with location table
        df = self.join_location_table(df,'u_site')

        # Step 6: Filters passed records
        df = self.filter_quality_result(df,partition_column_drop=['created_year','created_month'])

        # Step 7: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 8: Drops unnecessary columns
        df = self.drop_columns_for_processed(df)

        return df

        # Step 9: Selecting Columns that I want to take to processed layer
 

        # Step 10. Changes column names and schema
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
                merge_columns = ['change_number','sys_created_timestamp','restaurant_name','created_year','created_month']
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
    def process_change_request(self, df, column_name, location_df):
        """
        Process change request by exploding the DataFrame based on the provided column_name and joining it with location data.

        Args:
            df (DataFrame): The input DataFrame.
            column_name (str): The name of the column to process.
            location_df (DataFrame): The location DataFrame.

        Returns:
            DataFrame: The processed DataFrame.
        """
        # Alias the conflicting column in location_df
        location_df = location_df.withColumnRenamed("restaurant_full_name", "location_restaurant_full_name")

        # Extracting all stores with a comma in their name from locations DataFrame
        filtered_rows = location_df.filter(location_df['location_restaurant_full_name'].contains(','))
        filtered_rows = filtered_rows.select("location_restaurant_full_name").collect()
        all_store_commas = array([lit(row['location_restaurant_full_name']) for row in filtered_rows])

        # Broadcast the location DataFrame to all worker nodes
        broadcasted_location_df = broadcast(location_df.select("location_restaurant_full_name", "restaurant_id", "restaurant_name"))

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

        # Select only the necessary columns from the input DataFrame
        df = df.select(column_name)

        # Create a new column '_new' with the same value as the specified column
        df = df.withColumn(f"{column_name}_new", generate_combined_list_udf(df[column_name], all_store_commas))

        # Explode the column_new array into rows
        df = df.drop(column_name)
        df = df.withColumn(column_name, explode(df[f"{column_name}_new"]))
        df = df.drop(f"{column_name}_new")

        # Select only the necessary columns from the broadcasted location DataFrame
        broadcasted_location_df = broadcasted_location_df.select("location_restaurant_full_name", "restaurant_id", "restaurant_name")

        # Join with location data
        processed_df = df.join(broadcasted_location_df, df[column_name] == broadcasted_location_df["location_restaurant_full_name"], "left")

        return processed_df