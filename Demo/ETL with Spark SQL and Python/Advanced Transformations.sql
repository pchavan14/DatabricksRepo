-- Databricks notebook source
-- MAGIC %run /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

-- COMMAND ----------

-- MAGIC %fs  /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

-- COMMAND ----------

select * from customers-json;

-- COMMAND ----------

select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

create table customers 
as
select * from json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

select * from customers

-- COMMAND ----------

select count(*) from customers

-- COMMAND ----------

select profile:first_name , profile:last_name, profile:gender
from customers

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers

-- COMMAND ----------

create or replace temp view customers_final as
select customer_id , profile_struct.*
from parsed_customers

-- COMMAND ----------

select * from customers_final

-- COMMAND ----------

select * from customers_final

-- COMMAND ----------

select order_id , customer_id, books
from orders

-- COMMAND ----------

 select order_id , customer_id , explode(books) from orders

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books_csv b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions

-- COMMAND ----------

--higher order and UDF functions
SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS many_books
FROM orders

-- COMMAND ----------

SELECT order_id, many_books
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS many_books
  FROM orders)
WHERE size(many_books) > 0;

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    k -> CAST(k.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

---creating UDF's


 


