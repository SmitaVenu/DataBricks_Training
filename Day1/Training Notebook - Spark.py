# Databricks notebook source
print("sql")

# COMMAND ----------

data = [(1, 'a', 100), (2, 'b', 200)]
schema=["id","name","age"]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data = [(1, 'a', 100), (2, 'b', 200)]
schema="id int,name string,age int"
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe functions
# MAGIC
# MAGIC .select("*") --> for the full table
# MAGIC
# MAGIC df.select("id","age") --> like query results we are not saving it anywhere
# MAGIC
# MAGIC remove display whenever u trying to save it, or else it will be like we are taking an action on it , we wont be able to take any more action on it
# MAGIC
# MAGIC wheever we use . in pyspark it means we are using a function
# MAGIC
# MAGIC .alias
# MAGIC
# MAGIC .withcolumnrenamed
# MAGIC
# MAGIC .withcolumnsrenamed
# MAGIC
# MAGIC .withcolumn --> give a new column or replace the existing column --> depends on the column name in the query
# MAGIC
# MAGIC Function(py spark inbuilt function)
# MAGIC importing all the functions:
# MAGIC from pyspark.sql.functions import *
# MAGIC col --> converting one type to col datatype
# MAGIC
# MAGIC df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.select("*")

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("id")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select("id".alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

display(df.select(col("id").alias("emp_id")))

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()
df.withColumnsRenamed({"name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

help(df.withColumnsRenamed)

# COMMAND ----------

df.withColumn("current_date",current_date()).display()
df.withColumn("age",current_timestamp()).display()
