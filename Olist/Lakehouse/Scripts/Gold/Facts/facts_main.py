# Importing the logging & random module for pipeline logging
import logging
import random


# Pipeline log variables
job_id = random.getrandbits(16)
job_name = 'Olist_Gold_Facts_Job'


# Logger name & level configurations
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Logger format & file handler configurations
f = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
fh = logging.FileHandler(f"olistgoldfacts_{job_id}")
fh.setFormatter(f)
logger.addHandler(fh)


# Pipeline log
logger.info(f"JOB_ID   : {job_id}")
logger.info(f"JOB_NAME : {job_name}")
logger.info(f"The {job_name} pipeline is started.")


logger.info("Importing required modules for the pipeline.")

# Importing the required modules
import metadata, bq_utils, spark_utils
from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError

logger.info("Modules imported successfully.")


# Defining the main function
def main():
    
    logger.info(f"In the {__name__} function.")

    # Object creation for the user defined modules
    logger.info("Creating objects for the user defined modules.")
    meta_data = metadata.ReadJson()
    bq_utility = bq_utils.bqUtilities()
    spark_utility = spark_utils.SparkUtils()
    logger.info("Objects created successfully.")

    # Creating facts metadata list from the json
    facts_meta = meta_data.get_metadata()


    # Performing Data Loading
    try:
        # Establishing sparksession using spark_utility object
        logger.info("Establishing sparksession.")                
        spark = spark_utility.establish_spark_session()        
        logger.info("sparksession established successfully.")

        # Reading the source & dimension tables for loading the fact table
        dim_olist_date = spark_utility.dim_olist_date(spark)
        dim_olist_customers = spark_utility.dim_olist_customers(spark)
        dim_olist_orders = spark_utility.dim_olist_orders(spark)
        dim_olist_products = spark_utility.dim_olist_products(spark)
        dim_olist_order_items = spark_utility.dim_olist_order_items(spark)
        silver_olist_order_payments = spark_utility.silver_olist_order_payments(spark)


        # Looping through the Facts metadata list to perform data loading 
        for facts in facts_meta:
            try:
                # fact table variables
                fact_sys_name = facts['sys_name']
                fact_app_nm = facts['app_nm']
                fact_table_name = facts['table_name']
                fact_table_path = facts['basePath']
                fact_recordKey = facts['recordKey']
                fact_partitionKey = facts['partitionKey']
                fact_precombineKey = facts['precombineKey']
                fact_operationType = facts['operationType']
                        
                # Fetching the row count in fact table before data load
                logger.info(f"Fetching the row count in {fact_table_name} target table before data load.")
                row_count_pre_load = spark_utility.pre_load_fact_count(spark, fact_table_path)
                logger.info("Row count fetched successfully.")                        

                # Updating the pipeline log table in BigQuery for data load Start log
                logger.info("Updating data load Start log in bigquery pipeline_log table.")
                bq_utility.pipeline_start_log(job_id, job_name, fact_sys_name, fact_app_nm, fact_table_name, row_count_pre_load)
                logger.info("Log table updated.")                


                # Data loading with user defined transformation methods in spark_utils module
                logger.info(f"Performing data loading for the {fact_app_nm} application.")

                if fact_app_nm == 'Sales':
                    df = spark_utility.fact_olist_sales(dim_olist_date, dim_olist_customers, dim_olist_orders, dim_olist_products, dim_olist_order_items)
                    
                elif fact_app_nm == 'Order_Payments':
                    df = spark_utility.fact_olist_order_payments(dim_olist_date, dim_olist_orders, silver_olist_order_payments)

                logger.info(f"Data loading completed successfully for {fact_app_nm} application.")
                        
                # Gold hudi fact table configuration
                logger.info(f"Creating Hudi table configuration for the {fact_table_name} table.")        
                fact_table_config = spark_utility.hudi_options(fact_table_name, fact_recordKey, fact_partitionKey, fact_operationType, fact_precombineKey)
                logger.info(f"fact_table_config = {fact_table_config}")

                # Writing data to hudi gold fact table
                logger.info(f"Writing data to the {fact_table_name} Hudi table from the transformed pyspark dataframe.")                
                spark_utility.load_gold_fact_table(df, fact_table_config, fact_table_path)        
                logger.info(f"Data written successfully to the {fact_table_name} Hudi table.")

                # Fetching the row count in fact table after data load
                logger.info(f"Fetching the row count in {fact_table_name} target table after data load.")
                row_count_post_load = spark_utility.post_load_fact_count(spark, fact_table_path)
                logger.info("Row count fetched successfully.")                        
                        
                # Updating the pipeline log table in BigQuery for data load End log
                logger.info("Updating data load End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, fact_sys_name, fact_app_nm, fact_table_name, True, row_count_post_load)
                logger.info("Log table updated.")                

            except AnalysisException as e:
                # Logging AnalysisException details
                logger.error(f"AnalysisException Message : {str(e)}")
                logger.exception("Exception Details: ")

                # Updating the pipeline log table in BigQuery for data load End log
                logger.info("Updating data load End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, fact_sys_name, fact_app_nm, fact_table_name, False, row_count_pre_load)
                logger.info("Log table updated.")                       

            except Py4JJavaError as e:
                # Logging Py4JJavaError details
                logger.error(f"Py4JJavaError Message : {str(e)}")
                logger.exception("Exception Details: ")
                
                # Updating the pipeline log table in BigQuery for data load End log
                logger.info("Updating data load End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, fact_sys_name, fact_app_nm, fact_table_name, False, row_count_pre_load)
                logger.info("Log table updated.")                       

        logger.info(f"The data pipeline {job_name} has been completed.")

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