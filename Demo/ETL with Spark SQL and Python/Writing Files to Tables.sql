-- Databricks notebook source
-- we can query external files and add them to tables
-- with json & parquet we create managed tables whereas with undefined schema files we have to explicitly create managed tables

create table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %run ../Copy-Datasets

-- COMMAND ----------

-- MAGIC %run /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

-- COMMAND ----------

create table orders as
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders
select * from parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------


