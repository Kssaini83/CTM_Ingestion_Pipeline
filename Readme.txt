1. Databricks notebooks have been created to perform the required ingestion
2. 01_create_sample_data - Notebook to create the directory structure, /data,/data/landing,/data/staging, and sample CSV and JSON files from the python code
3. 02_Ingest_CSV_data - Notebook will ingest daily CSV data into parquet format in staging area. The method uses spark dataframe to read from csv files and write into parquet file format.
   The notebook expects a date parameter in the format yyyymmdd to be passed. The code would check if the daily file for that date exists or not.
   If the file doesn't exists, it will raise an exception and wouldn't proceed further.
4. The ingestion checks that only the records where transaction_id, customer_id and product_id is not null are loaded into staging.
   If the data has null values for quantity and total_price, it is converted to '0'
5. Sales_date column field is converted into timestamp format.

6. 03_Ingest_JSON_data - Notebook will ingest the data from continuous feed of incoming JSON files. The ingestion use COPY INTO method to write data into an external table. The      location for the external table is the staging area where the parquet files are to be stored.
7. The external table has been defined with NOT NULL constraint on transaction_id, customer_id and product_id. And data that violates this constraint would be written into the 'badRecordsPath' file path.
8. Any files that are corrupt would not be loaded at all by virtue of the format option - 'ignoreCorruptFiles' = 'false'

Job Pipeline
9. The notebooks can be scheduled to run by creating a Job in databricks workflow. The multi node job cluster can be defined to have auto scaling ON for handling the workload.
10. CRON scheduler will trigger the job on daily basis.
11. A parameter for the rundate will be passed into the job using which the process can check if the daily files for that day has arrived in the landing area or not. Based on that the job can run further to process the daily files. This is the data parameter that will be passed onto the ingestion file - 02_Ingest_CSV_data
12. The Job can be configured to send a notification over e-mail about the failure or success of the flow.
13. The JSON ingestion job can be triggered to run once a day or multiple times a day based on the number of files that are continuously flowing into the landing area. If the JSON files are in the order of hundreds of thousands, then the JSON ingestion job can be scheduled to run thrice or four times in the day(or more). 
