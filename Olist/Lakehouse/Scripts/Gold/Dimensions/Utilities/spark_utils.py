# Importing the required PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *


class SparkUtils():
    
    # Method to create & return sparksession for the pipeline process
    def establish_spark_session(self):
        # Initializing SparkSession with the application name OlistGoldDimensions
        spark = SparkSession.builder.appName('OlistGoldDimensions').getOrCreate()  

        return spark        
            

    # Method to read the silver Hudi table & return the pyspark dataframe    
    def read_silver(self, spark, src_table_path, src_table_columns):

        # Reading the silver hudi table using spark & function args
        src_table = spark.read.format('hudi').load(src_table_path)

        # Fetching specified columns from the dataframe using columns list arg
        src_df = src_table.select(*src_table_columns)
                        
        return src_df


    # Method to read the Gold Hudi table & return the pyspark dataframe    
    def read_dimension(self, spark, tgt_table_path, tgt_table_columns):

        # Reading the gold hudi table using spark & function args
        tgt_table = spark.read.format('hudi').load(tgt_table_path)

        # Fetching specified columns from the dataframe using columns list arg
        tgt_df = tgt_table.select(*tgt_table_columns)
                        
        return tgt_df
    

    # Method to read the Gold Hudi dimension table pre data load & return the row count    
    def pre_load_tgt_count(self, spark, tgt_table_path):

        # Reading the gold hudi dim table using spark & function args
        tgt_table = spark.read.format('hudi').load(tgt_table_path)

        # Fetching the row count
        row_count = tgt_table.count()
                        
        return row_count
    

    # Method to read the Gold Hudi dimension table post data load & return the row count    
    def post_load_tgt_count(self, spark, tgt_table_path):

        # Reading the gold hudi dim table using spark & function args
        tgt_table = spark.read.format('hudi').load(tgt_table_path)

        # Fetching the row count
        row_count = tgt_table.count()
                        
        return row_count
        

    """
     Method to capture SCD2 changes in Customers dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_customers(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_customers_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), "customer_id") \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.customer_email_id, '') <> coalesce(src.customer_email_id, '')) or
                                            (coalesce(tgt.customer_state, '') <> coalesce(src.customer_state, '')) or 
                                            (coalesce(tgt.customer_state_code, '') <> coalesce(src.customer_state_code, '')))
                                        """).select(
                                    col("tgt.customer_skey").alias("customer_skey"),    
                                    col("tgt.customer_id").alias("customer_id"),
                                    col("src.customer_email_id").alias("customer_email_id"),
                                    col("src.customer_state").alias("customer_state"),
                                    col("src.customer_state_code").alias("customer_state_code"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_customers_upd = dim_customers_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), col("src.customer_skey") == col("tgt.customer_skey")) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.customer_skey").alias("customer_skey"),
                                col("tgt.customer_id").alias("customer_id"),
                                col("tgt.customer_email_id").alias("customer_email_id"),    
                                col("tgt.customer_state").alias("customer_state"),
                                col("tgt.customer_state_code").alias("customer_state_code"),
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_customers_tmp1 = src_df.alias("src") \
                        .join(tgt_df.alias("tgt"), "customer_id", "leftanti") \
                        .select(
                            concat("src.customer_id", lit("_skey")).alias("customer_skey"),
                            col("src.customer_id").alias("customer_id"),
                            col("src.customer_email_id").alias("customer_email_id"),
                            col("src.customer_state").alias("customer_state"),
                            col("src.customer_state_code").alias("customer_state_code"),
                            current_date().alias("eff_start_dt"),
                            lit("9999-12-31").cast("date").alias("eff_end_dt"),
                            lit(1).alias("is_current"),
                            current_date().alias("insert_date"),
                            lit("olist_dev_elt").alias("inserted_by"),
                            lit(None).cast("string").alias("update_date"),
                            lit(None).cast("string").alias("updated_by")
                            )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_customers_tmp1.union(dim_customers_tmp).union(dim_customers_upd)

        return dim_upsert
    

    """
     Method to capture SCD2 changes in Orders dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_orders(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_orders_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.customer_id") == col("tgt.customer_id"))) \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.order_status, '') <> coalesce(src.order_status, '')) or
                                            (coalesce(tgt.order_purchase_timestamp, '') <> coalesce(src.order_purchase_timestamp, '')) or 
                                            (coalesce(tgt.order_approved_at, '') <> coalesce(src.order_approved_at, '')) or
                                            (coalesce(tgt.order_delivered_carrier_date, '') <> coalesce(src.order_delivered_carrier_date, '')) or                                    
                                            (coalesce(tgt.order_delivered_customer_date, '') <> coalesce(src.order_delivered_customer_date, '')) or
                                            (coalesce(tgt.order_estimated_delivery_date, '') <> coalesce(src.order_estimated_delivery_date, '')))
                                        """).select(
                                    col("tgt.order_skey").alias("order_skey"),  
                                    col("tgt.order_id").alias("order_id"),                            
                                    col("tgt.customer_skey").alias("customer_skey"),  
                                    col("tgt.customer_id").alias("customer_id"),
                                    col("src.order_status").alias("order_status"),
                                    col("src.order_purchase_timestamp").alias("order_purchase_timestamp"),
                                    col("src.order_approved_at").alias("order_approved_at"),
                                    col("src.order_delivered_carrier_date").alias("order_delivered_carrier_date"),
                                    col("src.order_delivered_customer_date").alias("order_delivered_customer_date"),
                                    col("src.order_estimated_delivery_date").alias("order_estimated_delivery_date"),                            
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_orders_upd = dim_orders_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_skey") == col("tgt.order_skey")) &
                                    (col("src.customer_skey") == col("tgt.customer_skey"))) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.order_skey").alias("order_skey"),
                                col("tgt.order_id").alias("order_id"),    
                                col("tgt.customer_skey").alias("customer_skey"),    
                                col("tgt.customer_id").alias("customer_id"),
                                col("tgt.order_status").alias("order_status"),    
                                col("tgt.order_purchase_timestamp").alias("order_purchase_timestamp"),
                                col("tgt.order_approved_at").alias("order_approved_at"),
                                col("tgt.order_delivered_carrier_date").alias("order_delivered_carrier_date"),
                                col("tgt.order_delivered_customer_date").alias("order_delivered_customer_date"),
                                col("tgt.order_estimated_delivery_date").alias("order_estimated_delivery_date"),                         
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_orders_tmp1 = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.customer_id") == col("tgt.customer_id")), "leftanti") \
                                .select(
                                    concat("src.order_id", lit("_skey")).alias("order_skey"),
                                    col("src.order_id").alias("order_id"),                            
                                    concat("src.customer_id", lit("_skey")).alias("customer_skey"),
                                    col("src.customer_id").alias("customer_id"),
                                    col("src.order_status").alias("order_status"),
                                    col("src.order_purchase_timestamp").alias("order_purchase_timestamp"),
                                    col("src.order_approved_at").alias("order_approved_at"),
                                    col("src.order_delivered_carrier_date").alias("order_delivered_carrier_date"),
                                    col("src.order_delivered_customer_date").alias("order_delivered_customer_date"),
                                    col("src.order_estimated_delivery_date").alias("order_estimated_delivery_date"),                            
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_orders_tmp1.union(dim_orders_tmp).union(dim_orders_upd)

        return dim_upsert


    """
     Method to capture SCD2 changes in Products dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_products(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_products_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.product_id") == col("tgt.product_id")) &
                                    (col("src.seller_id") == col("tgt.seller_id"))) \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.product_category_name, '') <> coalesce(src.product_category_name, '')) or
                                            (coalesce(tgt.product_category_name_english, '') <> coalesce(src.product_category_name_english, '')) or 
                                            (coalesce(tgt.price, '') <> coalesce(src.price, '')) or
                                            (coalesce(tgt.freight_value, '') <> coalesce(src.freight_value, '')) or                                    
                                            (coalesce(tgt.product_name_length, '') <> coalesce(src.product_name_length, '')) or
                                            (coalesce(tgt.product_description_length, '') <> coalesce(src.product_description_length, '')) or
                                            (coalesce(tgt.product_photos_qty, '') <> coalesce(src.product_photos_qty, '')) or
                                            (coalesce(tgt.product_weight_g, '') <> coalesce(src.product_weight_g, '')) or
                                            (coalesce(tgt.product_length_cm, '') <> coalesce(src.product_length_cm, '')) or
                                            (coalesce(tgt.product_height_cm, '') <> coalesce(src.product_height_cm, '')) or   
                                            (coalesce(tgt.product_width_cm, '') <> coalesce(src.product_width_cm, '')))
                                        """).select(
                                    col("tgt.product_skey").alias("product_skey"),  
                                    col("tgt.product_id").alias("product_id"),      
                                    col("tgt.seller_skey").alias("seller_skey"),      
                                    col("tgt.seller_id").alias("seller_id"),
                                    col("src.product_category_name").alias("product_category_name"),
                                    col("src.product_category_name_english").alias("product_category_name_english"),
                                    col("src.price").alias("price"),
                                    col("src.freight_value").alias("freight_value"),
                                    col("src.product_name_length").alias("product_name_length"),
                                    col("src.product_description_length").alias("product_description_length"),    
                                    col("src.product_photos_qty").alias("product_photos_qty"),    
                                    col("src.product_weight_g").alias("product_weight_g"),    
                                    col("src.product_length_cm").alias("product_length_cm"),    
                                    col("src.product_height_cm").alias("product_height_cm"),    
                                    col("src.product_width_cm").alias("product_width_cm"),                                
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_products_upd = dim_products_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.product_skey") == col("tgt.product_skey")) & 
                                    (col("src.seller_skey") == col("tgt.seller_skey"))) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.product_skey").alias("product_skey"),
                                col("tgt.product_id").alias("product_id"),     
                                col("tgt.seller_skey").alias("seller_skey"),    
                                col("tgt.seller_id").alias("seller_id"),
                                col("tgt.product_category_name").alias("product_category_name"),
                                col("tgt.product_category_name_english").alias("product_category_name_english"),
                                col("tgt.price").alias("price"),
                                col("tgt.freight_value").alias("freight_value"),
                                col("tgt.product_name_length").alias("product_name_length"),
                                col("tgt.product_description_length").alias("product_description_length"),    
                                col("tgt.product_photos_qty").alias("product_photos_qty"),    
                                col("tgt.product_weight_g").alias("product_weight_g"),    
                                col("tgt.product_length_cm").alias("product_length_cm"),    
                                col("tgt.product_height_cm").alias("product_height_cm"),    
                                col("tgt.product_width_cm").alias("product_width_cm"),                                
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_products_tmp1 = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.product_id") == col("tgt.product_id")) &
                                    (col("src.seller_id") == col("tgt.seller_id")), "leftanti") \
                                .select(
                                    concat("src.product_id", lit("_skey")).alias("product_skey"),
                                    col("src.product_id").alias("product_id"),                            
                                    concat("src.seller_id", lit("_skey")).alias("seller_skey"),
                                    col("src.seller_id").alias("seller_id"),
                                    col("src.product_category_name").alias("product_category_name"),
                                    col("src.product_category_name_english").alias("product_category_name_english"),
                                    col("src.price").alias("price"),
                                    col("src.freight_value").alias("freight_value"),
                                    col("src.product_name_length").alias("product_name_length"),
                                    col("src.product_description_length").alias("product_description_length"),    
                                    col("src.product_photos_qty").alias("product_photos_qty"),    
                                    col("src.product_weight_g").alias("product_weight_g"),    
                                    col("src.product_length_cm").alias("product_length_cm"),    
                                    col("src.product_height_cm").alias("product_height_cm"),    
                                    col("src.product_width_cm").alias("product_width_cm"),                                
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_products_tmp1.union(dim_products_tmp).union(dim_products_upd)

        return dim_upsert
    

    """
     Method to capture SCD2 changes in Sellers dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_sellers(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_sellers_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), "seller_id") \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.seller_state, '') <> coalesce(src.seller_state, '')) or 
                                            (coalesce(tgt.seller_state_code, '') <> coalesce(src.seller_state_code, '')))
                                        """).select(
                                    col("tgt.seller_skey").alias("seller_skey"),    
                                    col("tgt.seller_id").alias("seller_id"),
                                    col("src.seller_state").alias("seller_state"),
                                    col("src.seller_state_code").alias("seller_state_code"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_sellers_upd = dim_sellers_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), col("src.seller_skey") == col("tgt.seller_skey")) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.seller_skey").alias("seller_skey"),
                                col("tgt.seller_id").alias("seller_id"),
                                col("tgt.seller_state").alias("seller_state"),
                                col("tgt.seller_state_code").alias("seller_state_code"),
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_sellers_tmp1 = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), "seller_id", "leftanti") \
                                .select(
                                    concat("src.seller_id", lit("_skey")).alias("seller_skey"),
                                    col("src.seller_id").alias("seller_id"),
                                    col("src.seller_state").alias("seller_state"),
                                    col("src.seller_state_code").alias("seller_state_code"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_sellers_tmp1.union(dim_sellers_tmp).union(dim_sellers_upd)

        return dim_upsert


    """
     Method to capture SCD2 changes in Order_Items dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_order_items(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_order_items_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.product_id") == col("tgt.product_id")) & 
                                    (col("src.seller_id") == col("tgt.seller_id"))) \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.shipping_limit_date, '') <> coalesce(src.shipping_limit_date, '')) or
                                            (coalesce(tgt.shipping_year, '') <> coalesce(src.shipping_year, '')) or
                                            (coalesce(tgt.item_quantity, '') <> coalesce(src.item_quantity, '')) or                                    
                                            (coalesce(tgt.price, '') <> coalesce(src.price, '')) or 
                                            (coalesce(tgt.freight_value, '') <> coalesce(src.freight_value, '')))
                                        """).select(
                                    col("tgt.order_skey").alias("order_skey"),  
                                    col("tgt.order_id").alias("order_id"),                            
                                    col("tgt.product_skey").alias("product_skey"),  
                                    col("tgt.product_id").alias("product_id"),
                                    col("tgt.seller_skey").alias("seller_skey"),      
                                    col("tgt.seller_id").alias("seller_id"),                            
                                    col("src.shipping_limit_date").alias("shipping_limit_date"),
                                    col("src.shipping_year").alias("shipping_year"),    
                                    col("src.item_quantity").alias("item_quantity"),
                                    col("src.price").alias("price"),
                                    col("src.freight_value").alias("freight_value"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_order_items_upd = dim_order_items_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_skey") == col("tgt.order_skey")) &
                                    (col("src.product_skey") == col("tgt.product_skey")) & 
                                    (col("src.seller_skey") == col("tgt.seller_skey"))) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.order_skey").alias("order_skey"),
                                col("tgt.order_id").alias("order_id"),    
                                col("tgt.product_skey").alias("product_skey"),    
                                col("tgt.product_id").alias("product_id"),
                                col("tgt.seller_skey").alias("seller_skey"),                                
                                col("tgt.seller_id").alias("seller_id"),                            
                                col("tgt.shipping_limit_date").alias("shipping_limit_date"),
                                col("tgt.shipping_year").alias("shipping_year"),        
                                col("tgt.item_quantity").alias("item_quantity"),
                                col("tgt.price").alias("price"),
                                col("tgt.freight_value").alias("freight_value"),
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_order_items_tmp1 = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.product_id") == col("tgt.product_id")) & 
                                    (col("src.seller_id") == col("tgt.seller_id")), "leftanti") \
                                .select(
                                    concat("src.order_id", lit("_skey")).alias("order_skey"),
                                    col("src.order_id").alias("order_id"),                   
                                    concat("src.product_id", lit("_skey")).alias("product_skey"),    
                                    col("src.product_id").alias("product_id"),
                                    concat("src.seller_id", lit("_skey")).alias("seller_skey"),       
                                    col("src.seller_id").alias("seller_id"),                            
                                    col("src.shipping_limit_date").alias("shipping_limit_date"),
                                    col("src.shipping_year").alias("shipping_year"),    
                                    col("src.item_quantity").alias("item_quantity"),    
                                    col("src.price").alias("price"),
                                    col("src.freight_value").alias("freight_value"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_order_items_tmp1.union(dim_order_items_tmp).union(dim_order_items_upd)

        return dim_upsert
    

    """
     Method to capture SCD2 changes in Order_Ratings dimension table and 
     return updates & inserts as pyspark dataframe for dimension data loading
    """    
    def dim_order_ratings(self, src_df, tgt_df):

        #Creating dataframe for the active updates
        dim_order_ratings_tmp = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.rating_id") == col("tgt.rating_id"))) \
                                .filter("""(tgt.eff_end_dt = '9999-12-31') and 
                                        (tgt.is_current = 1) and 
                                        (src.last_update_date > tgt.eff_start_dt) and
                                        ((coalesce(tgt.rating_score, '') <> coalesce(src.rating_score, '')) or
                                            (coalesce(tgt.rating_survey_creation_date, '') <> coalesce(src.rating_survey_creation_date, '')) or 
                                            (coalesce(tgt.rating_survey_answer_timestamp, '') <> coalesce(src.rating_survey_answer_timestamp, '')))
                                        """).select(
                                    col("tgt.rating_skey").alias("rating_skey"),  
                                    col("tgt.rating_id").alias("rating_id"),                            
                                    col("tgt.order_skey").alias("order_skey"),  
                                    col("tgt.order_id").alias("order_id"),                            
                                    col("src.rating_score").alias("rating_score"),
                                    col("src.rating_survey_creation_date").alias("rating_survey_creation_date"),
                                    col("src.rating_survey_answer_timestamp").alias("rating_survey_answer_timestamp"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )
        
        #Creating dataframe for the inactive updates
        dim_order_ratings_upd = dim_order_ratings_tmp.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_skey") == col("tgt.order_skey")) & 
                                    (col("src.rating_skey") == col("tgt.rating_skey"))) \
                                .filter("""tgt.eff_end_dt = '9999-12-31' and
                                        tgt.is_current = 1
                                        """).select(
                                col("tgt.rating_skey").alias("rating_skey"),
                                col("tgt.rating_id").alias("rating_id"),                            
                                col("tgt.order_skey").alias("order_skey"),    
                                col("tgt.order_id").alias("order_id"),                            
                                col("tgt.rating_score").alias("rating_score"),
                                col("tgt.rating_survey_creation_date").alias("rating_survey_creation_date"),
                                col("tgt.rating_survey_answer_timestamp").alias("rating_survey_answer_timestamp"), 
                                col("tgt.eff_start_dt").alias("eff_start_dt"),
                                (current_date() - 1).alias("eff_end_dt"),
                                lit(0).alias("is_current"),
                                col("tgt.insert_date").alias("insert_date"),
                                col("tgt.inserted_by").alias("inserted_by"),
                                current_date().alias("update_date"),
                                lit("olist_dev_elt").alias("updated_by")
                                )
        
        #Creating dataframe for the new inserts
        dim_order_ratings_tmp1 = src_df.alias("src") \
                                .join(tgt_df.alias("tgt"), (col("src.order_id") == col("tgt.order_id")) &
                                    (col("src.rating_id") == col("tgt.rating_id")), "leftanti") \
                                .select(
                                    concat("src.rating_id", lit("_skey")).alias("rating_skey"),
                                    col("src.rating_id").alias("rating_id"),                   
                                    concat("src.order_id", lit("_skey")).alias("order_skey"),    
                                    col("src.order_id").alias("order_id"),                            
                                    col("src.rating_score").alias("rating_score"),
                                    col("src.rating_survey_creation_date").alias("rating_survey_creation_date"),
                                    col("src.rating_survey_answer_timestamp").alias("rating_survey_answer_timestamp"),
                                    current_date().alias("eff_start_dt"),
                                    lit("9999-12-31").cast("date").alias("eff_end_dt"),
                                    lit(1).alias("is_current"),
                                    current_date().alias("insert_date"),
                                    lit("olist_dev_elt").alias("inserted_by"),
                                    lit(None).cast("string").alias("update_date"),
                                    lit(None).cast("string").alias("updated_by")
                                    )

        #Combining all the updates & inserts as a single dataframe
        dim_upsert = dim_order_ratings_tmp1.union(dim_order_ratings_tmp).union(dim_order_ratings_upd)

        return dim_upsert


    # Method to create & return the Hudi table configurations for performing write operation to the hudi dimension tables in Gold layer
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


    # Method to write the hudi Gold Dimension table with the pyspark dataframe & the hudi_table_config variable
    def load_gold_dim_table(self, df, hudi_table_config, tgt_table_path):
        # Writing data to the table in the specified basepath        
        df.write.format("hudi"). \
        options(**hudi_table_config). \
        mode("append"). \
        save(tgt_table_path)

        return True