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


