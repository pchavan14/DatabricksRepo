-- Databricks notebook source
--Time travel feature in delta lakes
DESCRIBE HISTORY employees;

-- COMMAND ----------

--accessing data from previous versions
select * from employees
version as of 5;

-- COMMAND ----------

select * from employees@v5;

-- COMMAND ----------

DELETE FROM employees;

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

--restore data
RESTORE TABLE employees to version as of 6

-- COMMAND ----------

select * from employees

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

optimize employees
zorder by id;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

vacuum employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

select * from employees@v1

-- COMMAND ----------

drop table employees;

-- COMMAND ----------

 
