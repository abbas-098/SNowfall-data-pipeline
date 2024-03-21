from snowfall_pipeline.common_utilities.transform_base import TransformBase
from snowfall_pipeline.common_utilities.data_quality_rules import dq_rules
from delta.tables import DeltaTable


class ProcessedLocationHierarchy(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.dq_rule = dq_rules.get(self.datasets)
        self.file_path = "ods/location_hierarchy"
        self.list_of_files = self.aws_instance.get_files_in_s3_path(f"{self.raw_bucket_name}/{self.file_path}/")


    def get_data(self):
        df = self.read_data_from_s3(self.raw_bucket_name,self.file_path,'csv')
        return df


    def transform_data(self, df):
        return


    def save_data(self, df):
        return



