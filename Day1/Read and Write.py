# Databricks notebook source
# MAGIC %run /Workspace/Users/smita123venu@gmail.com/Day1/includes

# COMMAND ----------

df_sales=spark.read.json(f"{input_path}products.json")

# COMMAND ----------

df1_sales=add_injestion(df_sales)

# COMMAND ----------

df1_sales.write.mode("overwrite").saveAsTable("products")
