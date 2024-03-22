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

            'store_number': ('Store_Number', 'integer'),
            'store_name': ('Store_Name', 'string'),
            'latitude': ('Latitude', 'double'),
            'longitude': ('Longitude', 'double'),
            'address_line_1': ('Address_Line_1', 'string'),
            'address_line_2': ('Address_Line_2', 'string'),
            'address_line_3': ('Address_Line_3', 'string'),
            'address_line_4': ('Address_Line_4', 'string'),
            'postcode': ('Postcode', 'string'),
            'telephone': ('Telephone', 'string'),
            'nation': ('Nation', 'string'),
            'store_status': ('Store_Status', 'string'),
            'ownership_m_f': ('Ownership_M_F', 'string'),
            'coo_hierarchy_name': ('COO_Hierarchy_Name', 'string'),
            'coo_hierarchy_no': ('COO_Hierarchy_No', 'string'),
            'coo_full_name': ('COO_Full_Name', 'string'),
            'coo_surname': ('COO_Surname', 'string'),
            'coo_forename': ('COO_Forename', 'string'),
            'coo_known_as': ('COO_Known_As', 'string'),
            'coo_email': ('COO_Email', 'string'),
            'coo_eid': ('COO_Eid', 'string'),
            'coo_employee_no': ('COO_Employee_No', 'integer'),
            'rm_hierarchy_name': ('Rm_Hierarchy_Name', 'string'),
            'rm_hierarchy_no': ('Rm_Hierarchy_No', 'string'),
            'rm_full_name': ('Rm_Full_Name', 'string'),
            'rm_surname': ('Rm_Surname', 'string'),
            'rm_forename': ('Rm_Forename', 'string'),
            'rm_known_as': ('Rm_Known_As', 'string'),
            'rm_email': ('Rm_Email', 'string'),
            'rm_eid': ('Rm_Eid', 'string'),
            'rm_employee_no': ('Rm_Employee_No', 'integer'),
            'doo_dof_hierarchy_name': ('Doo_Dof_Hierarchy_Name', 'string'),
            'doo_dof_hierarchy_no': ('Doo_Dof_Hierarchy_No', 'string'),
            'doo_dof_full_name': ('Doo_Dof_Full_Name', 'string'),
            'doo_dof_surname': ('Doo_Dof_Surname', 'string'),
            'doo_dof_forename': ('Doo_Dof_Forename', 'string'),
            'doo_dof_known_as': ('Doo_Dof_Known_As', 'string'),
            'doo_dof_email': ('Doo_Dof_Email', 'string'),
            'doo_dof_eid': ('Doo_Dof_Eid', 'string'),
            'doo_dof_employee_no': ('Doo_Dof_Employee_No', 'integer'),
            'om_hierarchy_name': ('Om_Hierarchy_Name', 'string'),
            'om_hierarchy_no': ('Om_Hierarchy_No', 'string'),
            'om_full_name': ('Om_Full_Name', 'string'),
            'om_surname': ('Om_Surname', 'string'),
            'om_forename': ('Om_Forename', 'string'),
            'om_known_as': ('Om_Known_As', 'string'),
            'om_email': ('Om_Email', 'string'),
            'om_eid': ('Om_Eid', 'string'),
            'om_employee_no': ('Om_Employee_No', 'string'),
            'oc_fc_hierarchy_name': ('Oc_Fc_Hierarchy_Name', 'string'),
            'oc_fc_hierarchy_no': ('Oc_Fc_Hierarchy_No', 'string'),
            'oc_fc_full_name': ('Oc_Fc_Full_Name', 'string'),
            'oc_fc_surname': ('Oc_Fc_Surname', 'string'),
            'oc_fc_forename': ('Oc_Fc_Forename', 'string'),
            'oc_fc_known_as': ('Oc_Fc_Known_As', 'string'),
            'oc_fc_email': ('Oc_Fc_Email', 'string'),
            'oc_fc_eid': ('Oc_Fc_Eid', 'string'),
            'oc_fc_employee_no': ('Oc_Fc_Employee_No', 'string'),
            'oo_hierarchy_name': ('OO_Hierarchy_Name', 'string'),
            'oo_hierarchy_no': ('OO_Hierarchy_No', 'string'),
            'oo_full_name': ('OO_Full_Name', 'string'),
            'oo_surname': ('OO_Surname', 'string'),
            'oo_forename': ('OO_Forename', 'string'),
            'oo_known_as': ('OO_Known_As', 'string'),
            'oo_email': ('OO_Email', 'string'),
            'oo_eid': ('OO_Eid', 'string'),
            'oo_employee_no': ('OO_Employee_No', 'string'),
            'fs_hierarchy_name': ('Fs_Hierarchy_Name', 'string'),
            'fs_hierarchy_no': ('Fs_Hierarchy_No', 'string'),
            'fs_full_name': ('Fs_Full_Name', 'string'),
            'fs_surname': ('Fs_Surname', 'string'),
            'fs_forename': ('Fs_Forename', 'string'),
            'fs_known_as': ('Fs_Known_As', 'string'),
            'fs_email': ('Fs_Email', 'string'),
            'fs_eid': ('Fs_Eid', 'string'),
            'fs_employee_no': ('Fs_Employee_No', 'string')
        
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



