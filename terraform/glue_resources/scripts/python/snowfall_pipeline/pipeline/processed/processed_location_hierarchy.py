from snowfall_pipeline.common_utilities.transform_base import TransformBase
from delta.tables import DeltaTable

class ProcessedLocationHierarchy(TransformBase):

    def __init__(self, spark, sc, glueContext):
        super().__init__(spark, sc, glueContext)
        self.spark.conf.set("spark.sql.shuffle.partitions", "5") 
        self.pipeline_config = self.full_configs[self.datasets]
        self.file_path = "ods/location_hierarchy"


    def get_data(self):
        df = self.read_data_from_s3(self.preparation_bucket_name,self.file_path,'delta')
        return df


    def transform_data(self, df):
        """
        Transform the given DataFrame.

        This method executes the following steps:
        1. Removes trailing whitespaces
        2. Changes column names and schema
        3. Drops the data quality columns and CDC

        Parameters:
        - df: Input DataFrame.

        Returns:
        - DataFrame: Transformed DataFrame.

        """
        # Step 1: Removes trailing whitespaces
        df = self.remove_trailing_whitespace(df)

        column_mapping = {

            'store_number': ('store_sumber', 'integer'),
            'store_name': ('store_name', 'string'),
            'latitude': ('latitude', 'double'),
            'longitude': ('longitude', 'double'),
            'address_line_1': ('address_line_1', 'string'),
            'address_line_2': ('address_line_2', 'string'),
            'address_line_3': ('address_line_3', 'string'),
            'address_line_4': ('address_line_4', 'string'),
            'postcode': ('postcode', 'string'),
            'telephone': ('telephone', 'string'),
            'nation': ('nation', 'string'),
            'store_status': ('store_status', 'string'),
            'ownership_m_f': ('ownership_m_f', 'string'),
            'coo_hierarchy_name': ('coo_hierarchy_name', 'string'),
            'coo_hierarchy_no': ('coo_hierarchy_no', 'string'),
            'coo_full_name': ('coo_full_name', 'string'),
            'coo_surname': ('coo_surname', 'string'),
            'coo_forename': ('coo_forename', 'string'),
            'coo_known_as': ('coo_known_as', 'string'),
            'coo_email': ('coo_email', 'string'),
            'coo_eid': ('coo_eid', 'string'),
            'coo_employee_no': ('coo_employee_no', 'integer'),
            'rm_hierarchy_name': ('rm_hierarchy_name', 'string'),
            'rm_hierarchy_no': ('rm_hierarchy_no', 'string'),
            'rm_full_name': ('rm_full_name', 'string'),
            'rm_surname': ('rm_surname', 'string'),
            'rm_forename': ('rm_forename', 'string'),
            'rm_known_as': ('rm_known_As', 'string'),
            'rm_email': ('rm_email', 'string'),
            'rm_eid': ('rm_eid', 'string'),
            'rm_employee_no': ('rm_employee_no', 'string'),
            'doo_dof_hierarchy_name': ('doo_dof_hierarchy_name', 'string'),
            'doo_dof_hierarchy_no': ('doo_dof_hierarchy_no', 'string'),
            'doo_dof_full_name': ('doo_dof_full_name', 'string'),
            'doo_dof_surname': ('doo_dof_surname', 'string'),
            'doo_dof_forename': ('doo_dof_forename', 'string'),
            'doo_dof_known_as': ('doo_dof_known_As', 'string'),
            'doo_dof_email': ('doo_dof_email', 'string'),
            'doo_dof_eid': ('doo_dof_eid', 'string'),
            'doo_dof_employee_no': ('doo_dof_employee_no', 'string'),
            'om_hierarchy_name': ('om_hierarchy_name', 'string'),
            'om_hierarchy_no': ('om_hierarchy_no', 'string'),
            'om_full_name': ('om_full_name', 'string'),
            'om_surname': ('om_surname', 'string'),
            'om_forename': ('om_forename', 'string'),
            'om_known_as': ('om_known_as', 'string'),
            'om_email': ('om_email', 'string'),
            'om_eid': ('om_eid', 'string'),
            'om_employee_no': ('om_employee_no', 'string'),
            'oc_fc_hierarchy_name': ('oc_fc_hierarchy_name', 'string'),
            'oc_fc_hierarchy_no': ('oc_fc_hierarchy_no', 'string'),
            'oc_fc_full_name': ('oc_fc_full_name', 'string'),
            'oc_fc_surname': ('oc_fc_surname', 'string'),
            'oc_fc_forename': ('oc_fc_forename', 'string'),
            'oc_fc_known_as': ('oc_fc_known_as', 'string'),
            'oc_fc_email': ('oc_fc_email', 'string'),
            'oc_fc_eid': ('oc_fc_eid', 'string'),
            'oc_fc_employee_no': ('oc_fc_employee_no', 'string'),
            'oo_hierarchy_name': ('oO_hierarchy_name', 'string'),
            'oo_hierarchy_no': ('oo_hierarchy_no', 'string'),
            'oo_full_name': ('oo_full_name', 'string'),
            'oo_surname': ('oo_surname', 'string'),
            'oo_forename': ('oo_forename', 'string'),
            'oo_known_as': ('oo_known_as', 'string'),
            'oo_email': ('oo_email', 'string'),
            'oo_eid': ('oo_eid', 'string'),
            'oo_employee_no': ('oo_employee_no', 'string'),
            'fs_hierarchy_name': ('fs_hierarchy_name', 'string'),
            'fs_hierarchy_no': ('fs_hierarchy_no', 'string'),
            'fs_full_name': ('fs_full_name', 'string'),
            'fs_surname': ('fs_surname', 'string'),
            'fs_forename': ('fs_forename', 'string'),
            'fs_known_as': ('fs_known_as', 'string'),
            'fs_email': ('fs_email', 'string'),
            'fs_eid': ('fs_eid', 'string'),
            'fs_employee_no': ('fs_employee_no', 'string')
        
        }
        # 2. Changes column names and schema
        df = self.change_column_names_and_schema(df,column_mapping)

        # 3. Drops the data quality columns and CDC
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
            self.aws_instance.create_athena_delta_table('processed', 'ods_location_hierarchy', save_output_path, self.athena_output_path)
            
        else:

            # Merge data to the Delta table
            merge_columns = ['Store_Number','Store_Name']
            self.merge_to_delta_table(df,save_output_path,merge_columns)

        
        self.logger.info(f'Finished running the {self.__class__.__name__} pipeline!')



