-- Databricks notebook source
-- MAGIC %run /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

--read from the json file stored in dbfs and move it to delta table
select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_*.json`

-- COMMAND ----------

--we can also select data fro, directory given the partitioned files have same schema
select count(*) from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

--we can also add the input file name to records for analyzing issues in the data
select * , 
input_file_name() source_file 
from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

--we can use text format to query data from any file format
select * from text.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`

-- COMMAND ----------

--extract the data from csv format
select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv/`

-- COMMAND ----------

-- with file format not defined schema we need to create external tables
create table books_csv
(book_id string,title string, author string, category string, price double)
using csv
options (
 header = "true",
 delimiter = ";"
)
location "dbfs:/mnt/demo-datasets/bookstore/books-csv/"

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

 
