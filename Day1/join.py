# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_products=spark.table("products")

# COMMAND ----------

df_join = df_sales.join(df_customers,df_sales["customer_id"]==df_customers["customer_id"])
df_join.display()

# COMMAND ----------

df_join1=df_sales.join(df_products,df_sales["product_id"]==df_products["product_id"],"left")
df_join1.display()

# COMMAND ----------

df_customers.filter("customer_id=2 or customer_city='New Michaelview'").display()

# COMMAND ----------

from pyspark.sql.functions import *
df_customers.where(col("customer_id")==2).display()

# COMMAND ----------

df_customers.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_join.groupBy(df_sales["customer_id"]).count().orderBy("customer_id").display()
