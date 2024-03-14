import importlib
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from snowfall_pipeline.common_utilities.snowfall_logger import SnowfallLogger

logger = SnowfallLogger.get_logger()
  
class RunManager:

    def __init__(self, spark_context):
        # Initialise logging
        self.logger = SnowfallLogger.get_logger()
        self.spark_context = spark_context
        self.glue_context = GlueContext(self.spark_context)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)

    def run(self, group, dataset):
        #TODO ANY PRIOR STEPS BEFORE WE PROCESS
        run_grouping = getattr(self, f'run_{group}')
        run_grouping(dataset)

    def run_preparation(self, dataset): #TODO THIS IS WHERE OBSCURE EXCEPTIONS AND OUR RESOLUTION SHOULD TAKE PLACE, THEN CONTINURE OR END
        pipeline_instance = self.__fetch_pipeline_class__(group='preparation', dataset=dataset)
        pipeline_instance.process_flow()
        #run_processed(dataset) # Run processed after preparation

    def run_processed(self, dataset):
        pipeline_instance = self.__fetch_pipeline_class__(group='processed', dataset=dataset)
        pipeline_instance.process_flow()

    def run_semantic(self, dataset):
        pipeline_instance = self.__fetch_pipeline_class__(group='semantic', dataset=dataset)
        pipeline_instance.process_flow()

    def __fetch_pipeline_class__(self, group, dataset):
        try:
            module = importlib.import_module(f"snowfall_pipeline.pipeline.{group}.{group}_{dataset}" )
            pipeline_class = getattr(module, self.snake_to_camel(f"{group}_{dataset}"))
            return pipeline_class(self.spark, self.spark_context, self.glue_context)
        except AttributeError as e:
            #TODO WHATEVER NEEDS TO HAPPEN HERE
            sys.exit("Exiting the code with sys.exit() as no correct module could be found!")

    def snake_to_camel(self, snake_str): # TODO Probably shouldnt be here
        components = snake_str.split('_')
        return ''.join(x.title() for x in components)

def main():
    group = "processed" #TODO THIS WOULD BE WHERE YOU GET YOUR ENV VARS
    dataset = "incident_intraday"
    sc = SparkContext.getOrCreate()
    
    try:
        run_manager = RunManager(sc)
        run_manager.run(group=group, dataset=dataset)
    except Exception as e: #TODO ULTIMATE EXCEPTION CATCH FOR ANYTHING OUTSIDE OF PROCESSING
        logger.critical(f"Unhandled exception occurred. Attempting to shut down spark context. Error details:")
        logger.exception(e)

if __name__ == "__main__":
    main()