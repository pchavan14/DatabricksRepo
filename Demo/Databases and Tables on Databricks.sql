-- Databricks notebook source
create table managed_default (width int, length int, height int);

-- COMMAND ----------

insert into managed_default values(3 int, 2 int, 1 int);

-- COMMAND ----------

describe extended managed_default;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

show tables

-- COMMAND ----------

create view view_apple_phones as select * from smartphones where brand = 'Apple'

-- COMMAND ----------

select * from view_apple_phones

-- COMMAND ----------

show tables

-- COMMAND ----------

create temporary view temp_view_apple_phones as select * from smartphones where brand = 'Apple'

-- COMMAND ----------

show tables

-- COMMAND ----------

create global temporary view global_view_apple_phones 
as select * from smartphones 
where year > 2020
order by year desc

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from global_temp.global_view_apple_phones

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

 
