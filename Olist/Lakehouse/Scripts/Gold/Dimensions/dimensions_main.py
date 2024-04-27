# Importing the logging & random module for pipeline logging
import logging
import random


# Pipeline log variables
job_id = random.getrandbits(16)
job_name = 'Olist_Gold_Dimensions_Job'


# Logger name & level configurations
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Logger format & file handler configurations
f = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
fh = logging.FileHandler(f"olistgolddims_{job_id}")
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

    # Creating Gold & Silver metadata list from the json
    silver_meta, gold_meta = meta_data.get_metadata()


    # Performing Data Loading
    try:
        # Establishing sparksession using spark_utility object
        logger.info("Establishing sparksession.")                
        spark = spark_utility.establish_spark_session()        
        logger.info("sparksession established successfully.")


        # Looping through the Silver metaadata list to perform data loading 
        for silver in silver_meta:
            try:
                # Source/Silver table variables
                src_sys_name = silver['sys_name']
                src_app_nm = silver['app_nm']
                src_table_name = silver['table_name']
                src_table_path = silver['tablePath']
                src_table_columns = silver['columns']

                # Looping through the Gold metadata list to perform data loading 
                for gold in gold_meta:
                    if src_app_nm == gold['app_nm']:
                        # Target/Gold table variables
                        tgt_sys_name = gold['sys_name']
                        tgt_app_nm = gold['app_nm']
                        tgt_table_name = gold['table_name']
                        tgt_table_path = gold['basePath']
                        tgt_table_columns = gold["columns"]
                        tgt_recordKey = gold['recordKey']
                        tgt_partitionKey = gold['partitionKey']
                        tgt_precombineKey = gold['precombineKey']
                        tgt_operationType = gold['operationType']
                        
                        # Fetching the row count in target/gold table before data load
                        logger.info(f"Fetching the row count in {tgt_table_name} target table before data load.")
                        row_count_pre_load = spark_utility.pre_load_tgt_count(spark, tgt_table_path)
                        logger.info("Row count fetched successfully.")                        

                        # Updating the pipeline log table in BigQuery for data load Start log
                        logger.info("Updating data load Start log in bigquery pipeline_log table.")
                        bq_utility.pipeline_start_log(job_id, job_name, tgt_sys_name, tgt_app_nm, tgt_table_name, row_count_pre_load)
                        logger.info("Log table updated.")                

                        # Creating pyspark source dataframe from silver table
                        logger.info(f"Creating pyspark source dataframe from {src_table_name} Hudi table.")
                        src_df = spark_utility.read_silver(spark, src_table_path, src_table_columns)
                        logger.info(f"Pyspark source dataframe created successfully.")

                        # Creating pyspark source dataframe from gold table
                        logger.info(f"Creating pyspark source dataframe from {tgt_table_name} Hudi table.")
                        tgt_df = spark_utility.read_dimension(spark, tgt_table_path, tgt_table_columns)
                        logger.info(f"Pyspark source dataframe created successfully.")

                        # Data SCD2 tranformation with user defined transformation methods in spark_utils module
                        logger.info(f"Performing SCD2 transformation for the {tgt_app_nm} application.")

                        if tgt_app_nm == 'Customers':
                            df = spark_utility.dim_customers(src_df, tgt_df)
                        
                        elif tgt_app_nm == 'Orders':
                            df = spark_utility.dim_orders(src_df, tgt_df)

                        elif tgt_app_nm == 'Products':
                            df = spark_utility.dim_products(src_df, tgt_df)

                        elif tgt_app_nm == 'Sellers':
                            df = spark_utility.dim_sellers(src_df, tgt_df)

                        elif tgt_app_nm == 'Order_Items':
                            df = spark_utility.dim_order_items(src_df, tgt_df)

                        elif tgt_app_nm == 'Order_Ratings':
                            df = spark_utility.dim_order_ratings(src_df, tgt_df)

                        logger.info(f"SCD2 Data transformation completed successfully for {tgt_app_nm} application.")
                        
                        # Gold hudi dimension table configuration
                        logger.info(f"Creating Hudi table configuration for the {tgt_table_name} table.")        
                        gold_table_config = spark_utility.hudi_options(tgt_table_name, tgt_recordKey, tgt_partitionKey, tgt_operationType, tgt_precombineKey)
                        logger.info(f"gold_table_config = {gold_table_config}")

                        # Writing data to hudi gold dimension table
                        logger.info(f"Writing data to the {tgt_table_name} Hudi table from the transformed pyspark dataframe.")                
                        spark_utility.load_gold_dim_table(df, gold_table_config, tgt_table_path)        
                        logger.info(f"Data written successfully to the {tgt_table_name} Hudi table.")

                        # Fetching the row count in target/silver table after data load
                        logger.info(f"Fetching the row count in {tgt_table_name} target table after data load.")
                        row_count_post_load = spark_utility.post_load_tgt_count(spark, tgt_table_path)
                        logger.info("Row count fetched successfully.")                        
                        
                        # Updating the pipeline log table in BigQuery for data load End log
                        logger.info("Updating data load End log in bigquery pipeline_log table.")
                        bq_utility.pipeline_end_log(job_id, job_name, tgt_sys_name, tgt_app_nm, tgt_table_name, True, row_count_post_load)
                        logger.info("Log table updated.")                

            except AnalysisException as e:
                # Logging AnalysisException details
                logger.error(f"AnalysisException Message : {str(e)}")
                logger.exception("Exception Details: ")

                # Updating the pipeline log table in BigQuery for data load End log
                logger.info("Updating data load End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, tgt_sys_name, tgt_app_nm, tgt_table_name, False, row_count_pre_load)
                logger.info("Log table updated.")                       

            except Py4JJavaError as e:
                # Logging Py4JJavaError details
                logger.error(f"Py4JJavaError Message : {str(e)}")
                logger.exception("Exception Details: ")
                
                # Updating the pipeline log table in BigQuery for data load End log
                logger.info("Updating data load End log in bigquery pipeline_log table.")
                bq_utility.pipeline_end_log(job_id, job_name, tgt_sys_name, tgt_app_nm, tgt_table_name, False, row_count_pre_load)
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