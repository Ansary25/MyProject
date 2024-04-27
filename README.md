# <p align="center">Olist E-commerce Data LakeHouse Project</p>

In this project, we'll create and maintain a Data Lakehouse on the Google Cloud Platform (GCP). This Data Lakehouse will consume data from an E-commerce company called Olist and perform metadata driven Extract, Load, Transform (ELT) processes on a daily basis. We'll utilize advanced technologies such as PySpark and Apache Hudi for data staging, storing, cleaning, and transformation. Additionally, we'll implement a Star schema data model, empowering business users and stakeholders to derive insights and answer key business questions more effectively.
<br>
<br>

## Pipeline Architecture Diagram

![Copy of Olist E-comm DataLakeHouse Pipeline](https://github.com/Ansary25/MyProjectRepository/assets/93308341/38127522-01ae-4298-915f-5875a6c439eb)

###### <p align="center">fig 1.1 Olist E-commerce Data LakeHouse Pipeline Architecture</p>
<br>

## HUDI Data LakeHouse

### HUDI

Apache Hudi (pronounced “hoodie”) is the next generation streaming data lake platform. Apache Hudi brings core warehouse and database functionality directly to a data lake. Hudi provides tables, transactions, efficient upserts/deletes, advanced indexes, streaming ingestion services, data clustering/compaction optimizations, and concurrency all while keeping your data in open source file formats.

It allows us to create efficient incremental batch pipelines. Apache Hudi can easily be used on any cloud storage platform.


### LakeHouse Architecture

The data lakehouse is a open data management architecture combines the flexibility, cost-efficiency, and scale of data lakes with the data management and ACID transactions of data warehouses, enabling business intelligence (BI) and machine learning (ML) on all data.

The pipeline architecture is used to logically organize data in the lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables).

#### Bronze layer (raw data)
The Bronze layer is where we're going to perform data staging/data ingestion with the data from the external data source system/landing zone. 
The table structures in this layer correspond to the source system table structures "as-is," along with the following additional metadata columns "uuid", "process_id", "load_date" that create a temporary primary key uuid and capture the load date/time, process ID. 
The focus in this layer is to perform full load data ingestion from the landing zone.

#### Silver layer (cleansed and conformed data)
In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed, "just-enough" transformations and data cleansing rules are applied while loading the Silver layer.
Data transformations and data quality rules are applied here.

#### Gold layer (curated business-level tables)
Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases.
The Gold layer is for reporting and uses more de-normalized and read-optimized data model with fewer joins.
We use Kimball style star schema-based data model in this Gold Layer of the lakehouse.

So you can see that the data is curated as it moves through the different layers of a lakehouse. 

The data is stored in HUDI tables as Parquet open-source column-oriented data storage format in all the three layers.
<br>
<br>

## How it works

### Source Data System


In this project, our data source is a file system. We'll receive our data as CSV files for each application from the Olist E-commerce team. Please find below the entity relationship diagram of the data source.
<br>

![Olist E-comm DataLakeHouse Source Data Schema drawio](https://github.com/Ansary25/MyProjectRepository/assets/93308341/c695d1fa-5d9c-46ac-986f-1417acad0d9a)

###### <p align="center">fig 1.2 Olist E-commerce Source Data Entity Relationship Diagram</p>
<br>

The data source team from Olist will send the CSV file for 7 different applications. The applications are as below,
1. Customers - This dataset has the information about the customers
2. Orders - This dataset has the order transactions information
3. Products - This dataset has the information about the products
4. Sellers - This dataset has the information about the sellers
5. Order Items - This dataset has the order items information
6. Order Payments - This dataset has the information about the payment transactions
7. Order Ratings - This dataset has the order ratings information

### Landing Zone

We're utilizing Google Cloud Storage (GCS) bucket as the Landing Zone/Inbound area where the source data team will place the source files daily.
<br>
<br>

### HUDI DataLakeHouse
### Landing Zone to Bronze Layer
In this task, we'll perform data ingestion to the Bronze layer by extracting data files one by one from the landing zone with the help of metadata stored in a metadata table called "ing_metadata" in BigQuery.
The metadata table consists of the informations like "sys_name", "app_name", "load_order", "inbound_bucket", "raw_file_path", "raw_file_name", "raw_file_format", "delimiter", "bronze_file_path", "bronze_table_name".
It's helps in reading the source files and creating/overwriting the data into Hudi staging tables in Bronze layer.

Python & Pyspark scripts are used to perform this task with the help of GCP Dataproc.

Dataproc is a managed Spark and Hadoop service that lets us take advantage of open source data tools for batch processing, querying, streaming, and machine learning.

### Scripts used in Bronze layer
##### 1. Olist/Lakehouse/Scripts/Bronze/Utilities/bq_utils.py : 
This program helps us to read the metadata table in BigQuery with the help of bigquery module and convert it to an dataframe. Also it's used to call the predefined stored procedures in BigQuery which is helpful in updating the metadata table for the Start & Finish ingestion process status in metadata table as well as the "pipeline_log" table.

##### 2. Olist/Lakehouse/Scripts/Bronze/Utilities/spark_utils.py : 
This program is used for configuring Pyspark, creating sparksession and has methods which are used for reading the source file, adding audit columns, creating HUDI table configurations, loading the HUDI staging table. 

##### 3. Olist/Lakehouse/Scripts/Bronze/bronze_main.py : 
This is our main program, we'll be importing the above two programs as modules in this program. In this main script, pipeline logging configurations are setup initially for logging purpose. Then it'll loop through the metadata dataframe and perform the data ingestion for each application one by one respectively.

The ingestion logs are stored in the log table "pipeline_log" in BigQuery.
<br>
<br>

### Bronze to Silver Layer
In this task, we'll be performing data load from Bronze to Silver layer by reading the HUDI tables in the Bronze layer one by one with the help of metadata stored in a JSON file called "silver_metadata.json" in GCS bucket.
The JSON consists of the source and target table informations like "sys_name", "app_nm", "table_name", "tablePath", "columns", "timestamp_format", "recordKey", "partitionKey", "precombineKey", "operationType".
It's helps in reading the Bronze HUDI tables and performing data quality check, data deduplication, correcting data structure issues/data transformation then upsert/delta load to Hudi tables in Silver layer.

### Scripts used in Silver layer
##### 1. Olist/Lakehouse/Scripts/Silver/Utilities/metadata.py : 
This program helps us to read the Bronze & Silver metadata stored as JSON in GCS bucket with help of GCP storage module and returns it as python lists.

##### 2. Olist/Lakehouse/Scripts/Silver/Utilities/bq_utils.py : 
This script is used to call the predefined stored procedures in BigQuery which is helpful in updating the Start & Finish log status in "pipeline_log" table.

##### 3. Olist/Lakehouse/Scripts/Silver/Utilities/spark_utils.py : 
This program is used for configuring Pyspark, creating sparksession and has user-defined methods which are used for reading the source file, getting the target/silver table count pre & post data load to update in the log table, performing transformations with pre defined pyspark codes with respect to each application, creating HUDI table configurations, loading the HUDI Silver tables. 

##### 4. Olist/Lakehouse/Scripts/Silver/silver_main.py : 
This is our main program, we'll be importing the above three programs as modules in this program. 
In this main script, pipeline logging configurations are setup initially for logging purpose. Then it'll loop through the Bronze & Silver metadata list and perform the data transformation & loading for each application one by one respectively.
<br>
<br>


### Silver to Gold Layer
In this task, we'll be using Kimball style star schema-based data model and perform data loading to SCD2 dimension tables & fact tables from HUDI tables in the Silver layer.
Please find below the data model diagram for the Gold Layer. 
<br>

![Olist E-comm DataLakeHouse Dimensional Modelling drawio](https://github.com/Ansary25/MyProject/assets/93308341/a99c3c47-6e09-492f-bee6-b0489055af8a)

###### <p align="center">fig 1.3 Olist E-commerce Data LakeHouse Dimensional Modelling Diagram</p>
<br>

### Dimensions
Here, we need to perform data loading for the SCD2 dimension tables from Silver layer one by one with the help of metadata stored in a JSON file called "gold_metadata.json" in GCS bucket.
The JSON consists of the source and target table informations like "sys_name", "app_nm", "table_name", "tablePath", "columns", "timestamp_format", "recordKey", "partitionKey", "precombineKey", "operationType".
It's helps in reading the Silver HUDI tables and performing data load for Hudi Dimension tables in Gold layer.

### Scripts used in Gold layer for Dimension tables
##### 1. Olist/Lakehouse/Scripts/Gold/Dimensions/Utilities/metadata.py : 
This program helps us to read the Silver & Gold metadata stored as JSON in GCS bucket with help of GCP storage module and returns it as python lists.

##### 2. Olist/Lakehouse/Scripts/Gold/Dimensions/Utilities/bq_utils.py : 
This script is used to call the predefined stored procedures in BigQuery which is helpful in updating the Start & Finish log status in "pipeline_log" table.

##### 3. Olist/Lakehouse/Scripts/Gold/Dimensions/Utilities/spark_utils.py : 
This program is used for configuring Pyspark, creating sparksession and has methods which are used for reading the source/silver table, getting the target/dimension table count pre & post data load to update in the log table, performing transformations with pre defined pyspark codes for SCD2 operation with respect to each dimension table, creating HUDI table configurations, loading the HUDI Dimension tables. 

##### 4. Olist/Lakehouse/Scripts/Gold/Dimensions/dimensions_main.py : 
This is our main program, we'll be importing the above three programs as modules in this program. 
In this main script, pipeline logging configurations are setup initially for logging purpose. Then it'll loop through the Silver & Gold metadata list to perform the SCD2 data transformation & loading for each dimension table one by one respectively.
<br>

### Facts
Here, we need to perform data loading for the Fact tables from Gold layer one by one with the help of metadata stored in a JSON file called "gold_metadata.json" in GCS bucket.
The JSON consists of the target table informations like "sys_name", "app_nm", "table_name", "tablePath", "columns", "timestamp_format", "recordKey", "partitionKey", "precombineKey", "operationType".
It's helps in reading the Gold HUDI dimension & any source tables from Silver layer and performing data load for Hudi Fact tables in Gold layer.

### Scripts used in Gold layer for Fact tables
##### 1. Olist/Lakehouse/Scripts/Gold/Facts/Utilities/metadata.py : 
This program helps us to read the Fact table metadata stored as JSON in GCS bucket with help of GCP storage module and returns it as a list.

##### 2. Olist/Lakehouse/Scripts/Gold/Facts/Utilities/bq_utils.py : 
This script is used to call the predefined stored procedures in BigQuery which is helpful in updating the Start & Finish log status in "pipeline_log" table.

##### 3. Olist/Lakehouse/Scripts/Gold/Facts/Utilities/spark_utils.py : 
This program is used for configuring Pyspark, creating sparksession and has methods which are used for reading the dimension tables, getting the target/fact table count pre & post data load to update in the log table, performing joins & transformations with pre defined pyspark codes with respect to each fact table, creating HUDI table configurations, loading the HUDI Fact tables. 

##### 4. Olist/Lakehouse/Scripts/Gold/Facts/facts_main.py : 
This is our main program, we'll be importing the above three programs as modules in this program. 
In this main script, pipeline logging configurations are setup initially for logging purpose. Then it'll loop through the Fact table metadata list to perform the data transformation & loading for each fact table one by one respectively.
<br>
<br>

## Orchestration

### Dataproc WorkflowTemplate
Since it's a batch processing, we need to orchestrate the pipeline as a daily load. For orchestration we'll be using the Dataproc WorkflowTemplates with managed cluster, provides a flexible and easy-to-use mechanism for managing and executing workflows. 
It defines a graph of jobs with information on where to run those jobs. Instantiating a Workflow Template launches a Workflow. A Workflow is an operation that runs a Directed Acyclic Graph (DAG) of jobs on a cluster.

A workflow template can specify a managed cluster. The workflow will create an "ephemeral" cluster to run workflow jobs, and then delete the cluster when the workflow is finished

We'll create a pyspark job for each task with job dependencies, so that a job starts only after its dependencies complete successfully.
<br>
<br>

Please refer the scripts in the repository for more understanding.

I've done this project with the working experience in my previous organisation and learning.

# <p align="center">Thank You</p>
