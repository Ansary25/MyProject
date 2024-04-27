# Importing the required PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *


class SparkUtils():
    
    # Method to create & return sparksession for the pipeline process
    def establish_spark_session(self):
        # Initializing SparkSession with the application name OlistSilver
        spark = SparkSession.builder.appName('OlistSilver').getOrCreate()  

        return spark        
            

    # Method to read the bronze Hudi table & return the pyspark dataframe    
    def read_bronze(self, spark, src_table_path, src_table_columns):

        # Reading the bronze hudi table using spark & function args
        src_table = spark.read.format('hudi').load(src_table_path)

        # Fetching specified columns from the dataframe using columns list arg
        src_df = src_table.select(*src_table_columns)
                        
        return src_df


    # Method to read the Silver Hudi table pre data load & return the row count    
    def pre_load_tgt_count(self, spark, tgt_table_path):

        # Reading the silver hudi table using spark & function args
        tgt_table = spark.read.format('hudi').load(tgt_table_path)

        # Fetching the row count
        row_count = tgt_table.count()
                        
        return row_count
    

    # Method to read the Silver Hudi table post data load & return the row count    
    def post_load_tgt_count(self, spark, tgt_table_path):

        # Reading the silver hudi table using spark & function args
        tgt_table = spark.read.format('hudi').load(tgt_table_path)

        # Fetching the row count
        row_count = tgt_table.count()
                        
        return row_count
        

    """
     Method to perform data transformation in Customers bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def customers_transform(self, src_df):

        #DATA QUALITY CHECK
        df = src_df.filter("customer_id IS NOT NULL")        

        #DATA DEDUPLICATION
        customers_windowSpec = Window.partitionBy("customer_id").orderBy("customer_id")

        df = df.withColumn("row_number", row_number().over(customers_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")
        
        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        #CORRECTING DATASTRUCTURES ISSUES
        df = df.select(
                        upper(trim("customer_id")).alias("customer_id"),
                        lower(trim("customer_email_id")).alias("customer_email_id"),
                        initcap(trim("customer_state")).alias("customer_state"),
                        upper(trim("customer_state_code")).alias("customer_state_code"),
                        col("load_date").alias("last_update_date")
                        )
        
        return df, valid_data, invalid_data
    

    """
     Method to perform data transformation in Orders bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def orders_transform(self, src_df, src_timestamp_format):

        #DATA QUALITY CHECK
        df = src_df.filter("order_id is NOT NULL AND customer_id IS NOT NULL")

        #DATA DEDUPLICATION
        orders_windowSpec = Window.partitionBy("order_id", "customer_id").orderBy("order_id", "customer_id")

        df = df.withColumn("row_number", row_number().over(orders_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")
        
        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        #CORRECTING DATASTRUCTURES ISSUES
        df = df.select(
                        upper(trim("order_id")).alias("order_id"),
                        upper(trim("customer_id")).alias("customer_id"),
                        initcap(trim("order_status")).alias("order_status"),
                        to_timestamp("order_purchase_timestamp", src_timestamp_format).alias("order_purchase_timestamp"),
                        to_timestamp("order_approved_at", src_timestamp_format).alias("order_approved_at"),
                        to_timestamp("order_delivered_carrier_date", src_timestamp_format).alias("order_delivered_carrier_date"),
                        to_timestamp("order_delivered_customer_date", src_timestamp_format).alias("order_delivered_customer_date"),
                        to_date(to_timestamp("order_estimated_delivery_date", src_timestamp_format), "yyyy-MM-dd").alias("order_estimated_delivery_date"),
                        col("load_date").alias("last_update_date")
                        )

        return df, valid_data, invalid_data
    

    """
     Method to perform data transformation in Products bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def products_transform(self, src_df):

        #DATA QUALITY CHECK
        df = src_df.filter("product_id is NOT NULL AND seller_id IS NOT NULL")

        #DATA DEDUPLICATION
        products_windowSpec = Window.partitionBy("product_id", "seller_id").orderBy("product_id", "seller_id")

        df = df.withColumn("row_number", row_number().over(products_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")
        
        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        #CORRECTING DATASTRUCTURES ISSUES
        df = df.select(
                        upper(trim("product_id")).alias("product_id"),
                        upper(trim("seller_id")).alias("seller_id"),
                        initcap(trim("product_category_name")).alias("product_category_name"),
                        initcap(trim("product_category_name_english")).alias("product_category_name_english"),
                        col("price").cast("float").alias("price"),
                        col("freight_value").cast("float").alias("freight_value"),
                        col("product_name_length").cast("int").alias("product_name_length"),
                        col("product_description_length").cast("int").alias("product_description_length"),
                        col("product_photos_qty").cast("int").alias("product_photos_qty"),
                        col("product_weight_g").cast("float").alias("product_weight_g"),
                        col("product_length_cm").cast("float").alias("product_length_cm"),
                        col("product_height_cm").cast("float").alias("product_height_cm"),
                        col("product_width_cm").cast("float").alias("product_width_cm"),
                        col("load_date").alias("last_update_date")
                        )

        return df, valid_data, invalid_data
    

    """
     Method to perform data transformation in Sellers bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def sellers_transform(self, src_df):

        #DATA QUALITY CHECK
        df = src_df.filter("seller_id IS NOT NULL")

        #DATA DEDUPLICATION
        sellers_windowSpec = Window.partitionBy("seller_id").orderBy("seller_id")

        df = df.withColumn("row_number", row_number().over(sellers_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")
        
        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        #CORRECTING DATASTRUCTURES ISSUES
        df = df.select(
                        upper(trim("seller_id")).alias("seller_id"),
                        initcap(trim("seller_state")).alias("seller_state"),
                        upper(trim("seller_state_code")).alias("seller_state_code"),
                        col("load_date").alias("last_update_date")
                        )

        return df, valid_data, invalid_data


    """
     Method to perform data transformation in Order_Items bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def items_transform(self, src_df, src_timestamp_format):

        #DATA QUALITY CHECK
        df = src_df.filter("order_id is NOT NULL AND product_id is NOT NULL AND seller_id IS NOT NULL")

        #DATA DEDUPLICATION
        order_items_windowSpec = Window.partitionBy("order_id", "order_item_id", "product_id", "seller_id").orderBy("order_id", "order_item_id", "product_id", "seller_id")

        df = df.withColumn("row_number", row_number().over(order_items_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")

        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        df = df \
                .withColumn("order_id", upper(trim("order_id"))) \
                .withColumn("product_id", upper(trim("product_id"))) \
                .withColumn("seller_id", upper(trim("seller_id"))) \
                .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date", src_timestamp_format)) \
                .withColumn("shipping_year", year("shipping_limit_date")) \
                .withColumn("last_update_date", col("load_date")) \
                .groupBy(
                    "order_id",
                    "product_id",
                    "seller_id",
                    "shipping_limit_date",
                    "shipping_year",
                    "last_update_date"
                ).agg(
                    count("order_item_id").alias("item_quantity"),
                    round(sum("price"), 2).alias("price"),
                    round(sum("freight_value"), 2).alias("freight_value")
                )
        
        df = df.select(
                        "order_id",
                        "product_id",
                        "seller_id",
                        "shipping_limit_date",
                        "shipping_year",
                        "item_quantity",
                        "price",
                        "freight_value",
                        "last_update_date"                    
                )
        
        return df, valid_data, invalid_data


    """
     Method to perform data transformation in Order_Payments bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def payments_transform(self, src_df):

        #DATA QUALITY CHECK
        df = src_df.filter("order_id is NOT NULL AND payment_sequential is NOT NULL")

        #DATA DEDUPLICATION
        order_payments_windowSpec = Window.partitionBy("order_id", "payment_sequential").orderBy("order_id", "payment_sequential")

        df = df.withColumn("row_number", row_number().over(order_payments_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")        
        
        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        df = df.select(
                        upper(trim("order_id")).alias("order_id"),
                        col("payment_sequential").cast("int").alias("payment_sequential"),
                        initcap(trim(translate("payment_type", "_", " "))).alias("payment_type"),
                        col("payment_value").cast("float").alias("payment_value"),
                        col("load_date").alias("last_update_date")
                        )

        return df, valid_data, invalid_data
    

    """
     Method to perform data transformation in Order_Ratings bronze table and 
     return transformed pyspark dataframe with valid & invalid data count
    """    
    def ratings_transform(self, src_df, src_timestamp_format):

        #DATA QUALITY CHECK
        df = src_df.filter("rating_id is NOT NULL AND order_id is NOT NULL")

        #DATA DEDUPLICATION
        order_ratings_windowSpec = Window.partitionBy("rating_id", "order_id").orderBy("rating_id", "order_id")

        df = df.withColumn("row_number", row_number().over(order_ratings_windowSpec)) \
                .filter("row_number = 1") \
                .drop("row_number")

        #rating_score validation
        df = df.filter(col("rating_score").between(1,5))

        #VALID & INVALID DATA COUNT
        valid_data = df.count()
        invalid_data = src_df.subtract(df).count()

        df = df.select(
                        upper(trim("rating_id")).alias("rating_id"),    
                        upper(trim("order_id")).alias("order_id"),
                        col("rating_score").cast("float").alias("rating_score"),
                        to_date(to_timestamp("rating_survey_creation_date", src_timestamp_format), "yyyy-MM-dd").alias("rating_survey_creation_date"),
                        to_timestamp("rating_survey_answer_timestamp", src_timestamp_format).alias("rating_survey_answer_timestamp"),
                        col("load_date").alias("last_update_date")
                        )
        
        return df, valid_data, invalid_data


    # Method to create & return the Hudi table configurations for performing write operation to the hudi tables in Silver layer
    def hudi_options(self, tgt_table_name, tgt_recordKey, tgt_partitionKey, tgt_operationType, tgt_precombineKey):
        # creating configuration variable        
        hudi_table_config = {
                            'hoodie.table.name'                           : tgt_table_name,
                            'hoodie.datasource.write.recordkey.field'     : tgt_recordKey,
                            'hoodie.datasource.write.partitionpath.field' : tgt_partitionKey,
                            'hoodie.datasource.write.table.name'          : tgt_table_name,
                            'hoodie.datasource.write.operation'           : tgt_operationType,
                            'hoodie.datasource.write.precombine.field'    : tgt_precombineKey
                            }
        
        return hudi_table_config


    # Method to write the hudi silver table with the pyspark dataframe & the hudi_table_config variable
    def load_silver_table(self, df, hudi_table_config, tgt_table_path):
        # Writing data to the table in the specified basepath        
        df.write.format("hudi"). \
        options(**hudi_table_config). \
        mode("append"). \
        save(tgt_table_path)

        return True