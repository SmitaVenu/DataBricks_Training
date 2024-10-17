# Databricks notebook source
# DBTITLE 1,querying the files
# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table customers_sql as
# MAGIC select * from json.`/Volumes/databricks_training_smita/default/raw/customers.json`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table products_sql as
# MAGIC select * from json.`/Volumes/databricks_training_smita/default/raw/products.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table orders_sql as
# MAGIC select * from csv.`/Volumes/databricks_training_smita/default/raw/order_dates.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales_sql as
# MAGIC select * from csv.`/Volumes/databricks_training_smita/default/raw/sales.csv`
