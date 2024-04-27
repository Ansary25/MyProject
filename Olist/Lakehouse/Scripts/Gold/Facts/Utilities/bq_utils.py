# Importing bigquery module from google & sleep module
from google.cloud import bigquery
from time import sleep


class bqUtilities:

    def __init__(self):
        # Initializes the bqUtilities class with BigQuery client
        self.bq_client = bigquery.Client()
    

    # Method to insert the data load Start log in BigQuery pipeline_log table
    def pipeline_start_log(self, job_id, job_name, sys_name, app_nm, table_name, row_count):
        # Calling user defined bigquery procedure to perform log insert
        self.bq_client.query(f"call olist.pipeline_start_log({job_id}, '{job_name}', '{sys_name}', '{app_nm}', '{table_name}', {row_count});")
        sleep(2)

        return True
    

    # Method to insert the data load End log in BigQuery pipeline_log table
    def pipeline_end_log(self, job_id, job_name, sys_name, app_nm, table_name, flag, row_count):
        # Calling user defined bigquery procedure to perform log insert
        self.bq_client.query(f"call olist.pipeline_end_log({job_id}, '{job_name}', '{sys_name}', '{app_nm}', '{table_name}', {flag}, {row_count});")
        sleep(2)

        return True