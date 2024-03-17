from snowfall_pipeline.common_utilities.transform_base import TransformBase


class PreparationLocation(TransformBase):
    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.dq_rule = """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "sys_created_on"]"""
        self.file_path = "service_now/location"
        self.list_of_files = self.aws_instance.get_files_in_s3_path(f"{self.raw_bucket_name}/{self.file_path}/")


    def get_data(self):
        self.logger.info(f'Reading data in the file path: {self.raw_bucket_name}/{self.file_path}/')
        source_df = self.spark.read.json(f"{self.raw_bucket_name}/{self.file_path}/")
        appflow_row_number = self.aws_instance.extract_appflow_records_processed(self.list_of_files,self.pipeline_config['appflow_name'])
        self.logger.info(f'Number of records in dataframe: {source_df.count()}')
        if appflow_row_number is not None:
            self.logger.info(f'Number of records processed from appflow: {appflow_row_number}')
        return source_df

    def transform_data(self,df):
        self.logger.info('Removing duplicate records')
        df = df.dropDuplicates()
        self.logger.info(f'Number of records in dataframe after dropping duplicates: {df.count()}')
        # Running the data quality check
        df = self.data_quality_check(df,self.dq_rule,self.raw_bucket_name,self.file_path,'json')


    def save_data(self,df):
        "Abstract method which will be overridden when this class is inherited"
        pass
