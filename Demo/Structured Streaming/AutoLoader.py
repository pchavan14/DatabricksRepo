# Databricks notebook source
# MAGIC %run /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

  (spark.readStream
           .format("cloudFiles")
           .option("cloudFiles.format","parquet")
           .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/orders_checkpoint")
           .load(f"{dataset_bookstore}/orders-raw")
        .writeStream
           .option("checkpointLocation","dbfs:/mnt/demo/orders_checkpoint")
           .table("orders_update") 
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_update

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history orders_update

# COMMAND ----------


