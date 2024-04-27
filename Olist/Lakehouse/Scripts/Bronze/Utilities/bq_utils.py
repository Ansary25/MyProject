# Importing bigquery module from google & sleep module
from google.cloud import bigquery
from time import sleep


class bqUtilities:

    def __init__(self):
        # Initializes the bqUtilities class with BigQuery client
        self.bq_client = bigquery.Client()


    # Method to return the required metadata dataframe from the ing_metadata table in BigQuery for the active applications
    def get_metadata(self):        
        # Query to fetch the required metadata from the ing_metadata table in BigQuery
        meta_query = (
            f"SELECT sys_name, app_name, load_order, inbound_bucket, raw_file_path, raw_file_name, "
            f"raw_file_format, delimiter, bronze_file_path, bronze_table_name "
            f"FROM `GCPproject_id.olist.ing_metadata` "
            f"WHERE sys_name = 'Olist' AND is_enabled IS TRUE "
            f"ORDER BY load_order;"
        )

        # Query execution using bigquery client
        meta_query_job = self.bq_client.query(meta_query)

        # Converting the metadata object to dataframe
        meta_df = meta_query_job.to_dataframe()

        return meta_df


    # Method to update the ingestion Start status to Yet_To_Start in BigQuery ing_metadata table for the all active applications
    def ing_start_update(self):
        # Calling user defined bigquery procedure to perform update
        self.bq_client.query("call olist.ingestion_start_status_update();")
        sleep(2)

        return True
    

    # Method to update the ingestion Finish status in BigQuery ing_metadata table 
    def ing_finish_update(self, app_nm, load_odr, flag):
        # Calling user defined bigquery procedure to perform update
        self.bq_client.query(f"call olist.ingestion_finish_status_update('{app_nm}', {load_odr}, {flag});")
        sleep(2)

        return True
    

    # Method to insert the ingestion Start log in BigQuery pipeline_log table
    def pipeline_start_log(self, job_id, job_name, sys_name, app_nm, table_name, row_count):
        # Calling user defined bigquery procedure to perform log insert
        self.bq_client.query(f"call olist.pipeline_start_log({job_id}, '{job_name}', '{sys_name}', '{app_nm}', '{table_name}', {row_count});")
        sleep(2)

        return True
    

    # Method to insert the ingestion End log in BigQuery pipeline_log table
    def pipeline_end_log(self, job_id, job_name, sys_name, app_nm, table_name, flag, row_count):
        # Calling user defined bigquery procedure to perform log insert
        self.bq_client.query(f"call olist.pipeline_end_log({job_id}, '{job_name}', '{sys_name}', '{app_nm}', '{table_name}', {flag}, {row_count});")
        sleep(2)

        return True