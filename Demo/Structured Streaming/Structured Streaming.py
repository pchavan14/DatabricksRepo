# Databricks notebook source
# MAGIC %python
# MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}")
# MAGIC display(files)

# COMMAND ----------

# MAGIC %run /Repos/work.prachi.chavan@gmail.com/DatabricksRepo/Demo/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books 
# MAGIC as 
# MAGIC select * from csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv-new`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books

# COMMAND ----------

# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table books

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table books_csv rename to books

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books

# COMMAND ----------

(spark.readStream.table("books").createOrReplaceTempView("books_streaming_tmp_vw"))
         


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select author,count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author

# COMMAND ----------

#sorting not supported in streaming data
%sql
select * from books_streaming_tmp_vw
order by author

# COMMAND ----------

# MAGIC %sql
# MAGIC create or REPLACE temp view author_counts_tmp_vw as
# MAGIC (
# MAGIC select author, count(book_id) as total_books
# MAGIC from books_streaming_tmp_vw
# MAGIC group by author
# MAGIC )

# COMMAND ----------

_sqldf.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts_tmp_vw

# COMMAND ----------

(spark.table("author_counts_tmp_vw").writeStream.trigger(processingTime='4 seconds').outputMode("complete")
      .option("checkpointLocation","dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_counts

# COMMAND ----------


