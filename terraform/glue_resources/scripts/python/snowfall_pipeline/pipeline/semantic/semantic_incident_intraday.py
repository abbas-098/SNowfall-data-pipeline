from snowfall_pipeline.common_utilities.transform_base import TransformBase


class SemanticIncidentIntraday(TransformBase):


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
        print('Running process flow method')
        return