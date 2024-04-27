# Importing the logging & random module for pipeline logging
import logging
import random


# Pipeline log variables
job_id = random.getrandbits(16)
job_name = 'Olist_Bronze_Job'


# Logger name & level configurations
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Logger format & file handler configurations
f = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
fh = logging.FileHandler(f"olistbronze_{job_id}")
fh.setFormatter(f)
logger.addHandler(fh)


# Pipeline log
logger.info(f"JOB_ID   : {job_id}")
logger.info(f"JOB_NAME : {job_name}")
logger.info(f"The {job_name} for data ingestion pipeline is started.")


logger.info("Importing required modules for the pipeline.")

# Importing the required modules
import bq_utils, spark_utils
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

logger.info("Modules imported successfully.")


# Defining the main function
def main():
    
    logger.info(f"In the {__name__} function.")
    
    # Creating objects for the user defined modules
    logger.info("Creating objects for the user defined modules.")
    bq_utility = bq_utils.bqUtilities()
    spark_utility = spark_utils.SparkUtils()
    logger.info("Objects created successfully.")


    # Starting the data ingestion process
    try:
        # Establishing sparksession using spark_utility object
        logger.info("Establishing sparksession.")                
        spark = spark_utility.establish_spark_session()        
        logger.info("Sparksession established successfully.")

        # Ingestion 'Start' status update in bigquery ing_metadata table
        logger.info("Updating ingestion Start status to Yet_To_Start in bigquery ing_metadata table for the active applications.")
        bq_utility.ing_start_update()
        logger.info("Status updated.")

        # Generating metadata dataframe from BigQuery metadata table for ingestion process
        logger.info("Generating metadata dataframe from bigquery ing_metadata table.")
        meta_df = bq_utility.get_metadata()
        logger.info("Dataframe generated successfully.")


        # Looping through the metadata dataframe to perform ingestion for each active application
        for i in meta_df.index:
            try:
                # Pipeline variables from meta_df
                sys_name            =  meta_df["sys_name"][i]
                app_nm              =  meta_df["app_name"][i]
                load_odr            =  meta_df["load_order"][i]
                source_file_name    =  meta_df["raw_file_name"][i]
                source_file_path    =  meta_df["raw_file_path"][i]
                source_bucket       =  meta_df["inbound_bucket"][i]
                source_file_format  =  meta_df["raw_file_format"][i] 
                delimiter           =  meta_df["delimiter"][i]
                table_name          =  meta_df["bronze_table_name"][i]
                basePath            =  f'gs://{meta_df["bronze_file_path"][i]}'


                logger.info(f"Data Ingestion started for the application : {app_nm}.")

                logger.info(f'''
                        sys_name           : {sys_name}
                        app_name           : {app_nm}
                        load_order         : {load_odr}
                        source_file_name   : {source_file_name}
                        source_file_path   : {source_file_path}
                        source_bucket      : {source_bucket}
                        source_file_format : {source_file_format}
                        delimiter          : {delimiter}
                        bronze_table_name  : {table_name}
                        bronze_table_path  : {basePath}
                    ''')
            
                # Updating the pipeline log table in BigQuery for ingestion Start log
                logger.info("Updating ingestion Start log in bigquery pipeline_log table.")
                bq_utility.pipeline_start_log(job_id, job_name, sys_name, app_nm, table_name, 0)
                logger.info("Log table updated.")                

                # Creating PySpark DF from source file
                logger.info(f"Creating pyspark dataframe from source file {source_file_name}.")                        
                df = spark_utility.read_file(spark, delimiter, source_bucket, source_file_path, source_file_name)           
                logger.info(f"Pyspark dataframe created from the source file {source_file_name} successfully.")

                # Adding audit columns
                logger.info(f"Adding audit columns to the pyspark dataframe.")
                df_audit = spark_utility.add_audit_columns(df)
                logger.info(f"Audit columns added to pyspark dataframe successfully.")

                # Source data count
                row_count = df_audit.count()
                logger.info(f"Total number of rows in the file : {row_count}")

                # Creating the Hudi table configuration variable
                logger.info(f"Creating Hudi table configuration for the {table_name} table.")        
                hudi_table_config = spark_utility.hudi_options(table_name)
                logger.info(f"hudi_table_config = {hudi_table_config}")

                # Writing data to Hudi staging table
                logger.info(f"Writing data to the {table_name} Hudi table from the pyspark dataframe.")                
                spark_utility.load_stage_table(df_audit, hudi_table_config, basePath)        
                logger.info(f"Data written successfully to the {table_name} Hudi table.")

                # Updating the pipeline log table in BigQuery for ingestion End log
                logger.info("Updating ingestion End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, sys_name, app_nm, table_name, True, row_count)
                logger.info("Log table updated.")                

                # Updating ingestion 'Finish' status in bigquery ing_meta table
                logger.info(f"Updating the Finish ingestion status in bigquery ing_metadata table for the {app_nm} application.")
                bq_utility.ing_finish_update(app_nm, load_odr, True)
                logger.info("Status Updated.")

            except AnalysisException as e:
                # Logging AnalysisException details
                logger.error(f"AnalysisException Message : {str(e)}")
                logger.exception("Exception Details: ")

                # Updating the pipeline log table in BigQuery for ingestion End log
                logger.info("Updating ingestion End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, sys_name, app_nm, table_name, False, 0)
                logger.info("Log table updated.")                       

                # Updating ingestion 'Error' job_status in bigquery ing_metadata table
                logger.info(f"Updating the ingestion Error status in bigquery ing_metadata table for the {app_nm} application.")
                bq_utility.ing_finish_update(app_nm, load_odr, False)        
                logger.info("Status Updated.")

            except Py4JJavaError as e:
                # Logging Py4JJavaError details
                logger.error(f"Py4JJavaError Message : {str(e)}")
                logger.exception("Exception Details: ")
                
                # Updating the pipeline log table in BigQuery for ingestion End log
                logger.info("Updating ingestion End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, sys_name, app_nm, table_name, False, 0)
                logger.info("Log table updated.")                       

                # Updating ingestion 'Error' job_status in bigquery ing_metadata table
                logger.info(f"Updating the ingestion Error status in bigquery ing_metadata table for the {app_nm} application.")
                bq_utility.ing_finish_update(app_nm, load_odr, False)        
                logger.info("Status Updated.")

        logger.info(f"The data ingestion pipeline {job_name} has been completed.")

    except Py4JJavaError as e:
        # Logging Py4JJavaError details        
        logger.critical(f"The Py4JJavaError Message is : {str(e)}")
        logger.critical("Exception Details: ")

    except Exception as e:
        # Logging Exception details
        logger.critical(f"An unexpected error occurred: {str(e)}")
        logger.critical("Exception Details: ")

    finally:
        # Stopping the sparksession
        logger.info("Stopping the SparkSession.")
        spark.stop()
        logger.info("SparkSession has been stopped successfully.")

        logger.info("---------------------------------------xxx--------------------------------------------")


# Executing the main function
if __name__ == '__main__':
    main()