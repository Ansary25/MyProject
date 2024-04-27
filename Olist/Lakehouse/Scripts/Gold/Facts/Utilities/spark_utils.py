# Importing the required PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import *


class SparkUtils():
    
    # Method to create & return sparksession for the pipeline process
    def establish_spark_session(self):
        # Initializing SparkSession with the application name OlistGoldFacts
        spark = SparkSession.builder.appName('OlistGoldFacts').getOrCreate()  

        return spark        
            

    # Method to read the dim_olist_date Hudi table & return it as a pyspark dataframe    
    def dim_olist_date(self, spark):

        # Reading the dim_olist_date hudi table using spark
        dim_olist_date = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Gold/Hudi/dim_olist_date/')
                       
        return dim_olist_date


    # Method to read the dim_olist_customers Hudi table & return it as a pyspark dataframe    
    def dim_olist_customers(self, spark):

        # Reading the dim_olist_customers hudi table using spark
        dim_olist_customers = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Gold/Hudi/dim_olist_customers/') \
                                        .filter("""(eff_end_dt = '9999-12-31') and
                                                    (is_current = 1)""")
                       
        return dim_olist_customers


    # Method to read the dim_olist_orders Hudi table & return it as a pyspark dataframe    
    def dim_olist_orders(self, spark):

        # Reading the dim_olist_orders hudi table using spark
        dim_olist_orders = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Gold/Hudi/dim_olist_orders/') \
                                     .filter("""(eff_end_dt = '9999-12-31') and
                                                (is_current = 1)""")
                       
        return dim_olist_orders

    
    # Method to read the dim_olist_products Hudi table & return it as a pyspark dataframe    
    def dim_olist_products(self, spark):

        # Reading the dim_olist_products hudi table using spark
        dim_olist_products = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Gold/Hudi/dim_olist_products/') \
                                       .filter("""(eff_end_dt = '9999-12-31') and
                                                  (is_current = 1)""")
                       
        return dim_olist_products


    # Method to read the dim_olist_order_items Hudi table & return it as a pyspark dataframe    
    def dim_olist_order_items(self, spark):

        # Reading the dim_olist_order_items hudi table using spark
        dim_olist_order_items = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Gold/Hudi/dim_olist_order_items/') \
                                          .filter("""(eff_end_dt = '9999-12-31') and
                                                     (is_current = 1)""")
                       
        return dim_olist_order_items
    

    # Method to read the silver_olist_order_payments Hudi table & return it as a pyspark dataframe    
    def silver_olist_order_payments(self, spark):

        # Reading the silver_olist_order_payments hudi table using spark
        silver_olist_order_payments = spark.read.format('hudi').load('gs://olist-ecomm-lakehouse/Silver/Hudi/silver_olist_order_payments/')
                       
        return silver_olist_order_payments
    

    # Method to read the Gold Hudi fact table pre data load & return the row count    
    def pre_load_fact_count(self, spark, fact_table_path):

        # Reading the gold hudi fact table using spark & function args
        fact_table = spark.read.format('hudi').load(fact_table_path)

        # Fetching the row count
        row_count = fact_table.count()
                        
        return row_count
    

    # Method to read the Gold Hudi fact table post data load & return the row count    
    def post_load_fact_count(self, spark, fact_table_path):

        # Reading the gold hudi fact table using spark & function args
        fact_table = spark.read.format('hudi').load(fact_table_path)

        # Fetching the row count
        row_count = fact_table.count()
                        
        return row_count    
    

    """
     Method to perform joins with source dimension tables and 
     return the data as pyspark dataframe for fact_olist_sales data loading
    """    
    def fact_olist_sales(self, dim_olist_date, dim_olist_customers, dim_olist_orders, dim_olist_products, dim_olist_order_items):

        # DataFrame joining the source tables with user defined conditions
        fact_olist_sales = dim_olist_date.alias('dt') \
                            .join(dim_olist_orders.alias('o'),
                                (to_date(col('dt.date_skey'), 'yyyyMMdd') == date_trunc('day', col('o.order_purchase_timestamp').cast('timestamp')))) \
                            .join(dim_olist_customers.alias('c'),
                                (col('o.customer_skey') == col('c.customer_skey'))) \
                            .join(dim_olist_order_items.alias('oim'),
                                (col('o.order_skey') == col('oim.order_skey'))) \
                            .join(dim_olist_products.alias('p'),
                                (col('oim.product_skey') == col('p.product_skey'))) \
                            .where(col('o.order_status') == 'Delivered')

        # Fetching required fatc table columns from the source DF
        df = fact_olist_sales.select(
                col('dt.date_skey').alias('date_skey'),
                col('c.customer_skey').alias('customer_skey'),
                col('o.order_skey').alias('order_skey'),
                col('oim.product_skey').alias('product_skey'),
                col('oim.seller_skey').alias('seller_skey'),
                col('o.order_purchase_timestamp').alias('order_purchase_timestamp'),
                col('dt.cal_month_name').alias('order_purchase_month'),
                col('p.product_category_name_english').alias('product_category_name_english'),
                col('oim.item_quantity').alias('sales_quantity'),
                col('oim.price').alias('net_price'),
                col('oim.freight_value').alias('net_freight_value'),
                (col('oim.price') + col('oim.freight_value')).alias('net_sales_amount'),
                col('c.customer_state').alias('customer_state'),
                col('c.customer_state_code').alias('customer_state_code'),
                current_date().alias('load_date')
            )
        
        return df


    """
     Method to perform joins with source dimension tables and 
     return the data as pyspark dataframe for fact_olist_order_payments data loading
    """    
    def fact_olist_order_payments(self, dim_olist_date, dim_olist_orders, silver_olist_order_payments):

        # DataFrame joining the source tables with user defined conditions
        fact_olist_order_payments = dim_olist_date.alias('dt') \
                                        .join(dim_olist_orders.alias('o'),
                                            (to_date(col('dt.date_skey'), 'yyyyMMdd') == date_trunc('day', col('o.order_approved_at').cast('timestamp')))) \
                                        .join(silver_olist_order_payments.alias('op'),
                                            (col('o.order_skey') == concat('op.order_id', lit('_skey'))))

        # Fetching required fatc table columns from the source DF
        df = fact_olist_order_payments.select(
                col('dt.date_skey').alias('date_skey'),
                col('o.customer_skey').alias('customer_skey'),
                col('o.order_skey').alias('order_skey'),
                col('o.order_status').alias('order_status'),
                col('o.order_approved_at').alias('payment_approved_at'),
                col('dt.cal_month_name').alias('order_payment_month'),
                col('op.payment_sequential').alias('payment_sequential'),
                col('op.payment_type').alias('payment_type'),
                col('op.payment_value').alias('payment_value'),
                current_date().alias('load_date')
            )
        
        return df


    # Method to create & return the Hudi table configurations for performing write operation to the fact hudi tables in Gold layer
    def hudi_options(self, fact_table_name, fact_recordKey, fact_partitionKey, fact_operationType, fact_precombineKey):
        # creating configuration variable        
        hudi_table_config = {
                            'hoodie.table.name'                           : fact_table_name,
                            'hoodie.datasource.write.recordkey.field'     : fact_recordKey,
                            'hoodie.datasource.write.partitionpath.field' : fact_partitionKey,
                            'hoodie.datasource.write.table.name'          : fact_table_name,
                            'hoodie.datasource.write.operation'           : fact_operationType,
                            'hoodie.datasource.write.precombine.field'    : fact_precombineKey
                            }
        
        return hudi_table_config


    # Method to write the hudi Gold Fact table with the pyspark dataframe & the hudi_table_config variable
    def load_gold_fact_table(self, df, hudi_table_config, fact_table_path):
        # Writing data to the table in the specified basepath        
        df.write.format("hudi"). \
        options(**hudi_table_config). \
        mode("append"). \
        save(fact_table_path)

        return True