from snowfall_pipeline.common_utilities.transform_base import TransformBase


class PreparationLocations(TransformBase):
    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.dq_rule = """Rules = [
        ColumnCount = 76,
        RowCount > 0,
        IsComplete "sys_created_on"
    ]"""



    def get_data(self):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def transform_data(self):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def save_data(self):
        "Abstract method which will be overridden when this class is inherited"
        pass

    def process_flow(self):
        print('Running process flow method which is for the preparation class')
        return