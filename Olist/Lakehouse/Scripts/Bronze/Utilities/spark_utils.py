# Importing required PySpark modules & uuid module
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


class SparkUtils():
    
    # Method to create & return sparksession for the pipeline process
    def establish_spark_session(self):
        # Initializing SparkSession with the application name OlistBronze
        spark = SparkSession.builder.appName('OlistBronze').getOrCreate()  

        return spark        
            

    # Method to read the csv source files with read options & return the pyspark dataframe    
    def read_file(self, spark, delimiter, source_bucket, source_file_path, source_file_name):
        # Reading the source files using spark & function args
        df = spark.read \
            .options(delimiter=delimiter, header=True) \
            .csv(f"gs://{source_bucket}/{source_file_path}{source_file_name}")
        
        return df


    # Method to add audit columns (load_date, process_id) & primary key (uuid) to the pyspark dataframe returned by read_file method
    def add_audit_columns(self, df):
        # Adding audit columns to the pyspark dataframe
        df = df.withColumn("uuid", expr("uuid()")) \
                .withColumn("process_id", date_format(current_date(), "yyyyMMdd").cast("long")) \
                .withColumn("load_date", current_date())
        
        return df


    # Method to create & return the Hudi table configurations for performing write operation to the hudi tables in Bronze layer
    def hudi_options(self, table_name):
        # creating configuration variable
        hudi_table_config = {
                            'hoodie.table.name'                           : table_name,
                            'hoodie.datasource.write.recordkey.field'     : 'uuid',
                            'hoodie.datasource.write.partitionpath.field' : 'load_date',
                            'hoodie.datasource.write.table.name'          : table_name,
                            'hoodie.datasource.write.operation'           : 'insert',
                            'hoodie.datasource.write.precombine.field'    : 'load_date'
                            }
        
        return hudi_table_config


    # Method to write the hudi staging table with the pyspark dataframe & the hudi_table_config variable
    def load_stage_table(self, df, hudi_table_config, basePath):
        # Writing data to the table in the specified basepath
        df.write.format("hudi"). \
        options(**hudi_table_config). \
        mode("overwrite"). \
        save(basePath)

        return True