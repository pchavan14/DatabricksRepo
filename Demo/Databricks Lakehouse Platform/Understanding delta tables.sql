-- Databricks notebook source
--Creating delta tables

CREATE TABLE employees
(id int, name string, salary double);


-- COMMAND ----------

insert into employees (id,name,salary) values (5,"Anna",2500.0);

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

 update employees
 set salary = salary + 100
 where name like 'A%';

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log' 

-- COMMAND ----------

-- MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000000.json' 

-- COMMAND ----------


