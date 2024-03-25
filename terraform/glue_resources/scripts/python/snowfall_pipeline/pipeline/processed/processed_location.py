from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from snowfall_pipeline.common_utilities.decorators import transformation_timer

class ProcessedLocation(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "service_now/location"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Removes trailing whitespaces
        2. Splits JSON column
        3. Splits datetime column
        4. Splits location string
        5. Filters passed records
        6. Gets unique records
        7. Drops unnecessary columns

        Parameters:
        - df (DataFrame): Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.
        """
        # Step 1: Removes trailing whitespaces
        df = self.remove_trailing_whitespace(df)

        # Step 2: Splits JSON column
        df = self.split_json_column(df, self.pipeline_config.get('transform_json'))

        # Step 3: Splits datetime column
        df = self.split_datetime_column(df, self.pipeline_config.get('process_timestamp'))

        # Step 4: Splits location string
        df = self._transform_location_split(df, ['full_name'])

        # Step 5: Filters passed records
        df = self.filter_quality_result(df)

        # Step 6: Gets unique records
        df = self.get_unique_records_sql(df)

        # Step 7: Drops unnecessary columns
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
            self.aws_instance.create_athena_delta_table('processed', 'service_now_location', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['full_name','sys_created_on']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')




    @transformation_timer
    def _transform_location_split(self,df, column_names):
        """
        Apply the split location string transformation to specified columns in the DataFrame.

        Args:
            df (DataFrame): The input Spark DataFrame.
            column_names (list): List of column names to split location strings for.

        Returns:
            DataFrame: The DataFrame with split location columns.
        """
        self.logger.info('Running the transform_location_split function')
        for column_name in column_names:
            is_numeric = F.col(column_name).rlike("^[0-9]+$")
            is_alphabetic = F.col(column_name).rlike("^[a-zA-Z\s]+$")

            df = df.withColumn(
                f"{column_name}_restaurant_id",
                F.when(is_numeric | is_alphabetic, F.lit(-1))
                .otherwise(F.regexp_replace(F.col(column_name), r"([0-9]+)[^0-9]?.*", r"$1").cast("int"))
            ).withColumn(
                f"{column_name}_restaurant_name",
                F.when(is_numeric | is_alphabetic, F.col(column_name))
                .otherwise(F.regexp_replace(F.col(column_name), r"^[0-9]+[^0-9]?(.*)", r"$1"))
            ).withColumn(
                f"{column_name}_restaurant_name",
                F.when(F.col(f"{column_name}_restaurant_name").rlike("^[, -]"), 
                    F.regexp_replace(F.col(f"{column_name}_restaurant_name"), r"^[, -]+", ""))
                .otherwise(F.col(f"{column_name}_restaurant_name"))
            )

            df = df.withColumn(
                f"{column_name}_restaurant_id",
                F.when(F.col(f"{column_name}_restaurant_id").isNull(), F.lit(-1))
                .otherwise(F.col(f"{column_name}_restaurant_id"))
            )

        return df


    def get_unique_records_sql(self,df):
        """
        Run the SQL query on the dataframe.
        """
        df.createOrReplaceTempView("my_dataframe")
        query =  """
                SELECT a.*
                FROM my_dataframe a
                INNER JOIN (
                    SELECT full_name, MAX(to_timestamp(sys_updated_on, 'dd-MM-yyyy HH:mm:ss')) AS latest_timestamp
                    FROM my_dataframe
                    GROUP BY full_name
                ) b ON a.full_name = b.full_name AND 
                to_timestamp(a.sys_updated_on, 'dd-MM-yyyy HH:mm:ss') = b.latest_timestamp
                """
        return self.spark.sql(query)