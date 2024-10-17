# Databricks notebook source
simpleData = ((1,"James", "Sales", 3000), \
    (2,"Michael", "Sales", 4600),  \
    (3,"Robert", "Sales", 4100),   \
    (4,"Maria", "Finance", 3000),  \
    (5,"James", "Sales", 3000),    \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100),\
    (6,"Scott", "Finance", 3300),  \
    (7,"Jen", "Finance", 3900),    \
    (8,"Jeff", "Marketing", 3000), \
    (9,"Kumar", "Marketing", 2000),\
    (10,"Saif", "Sales", 4100), \
    (None,None,None,None),\
    (None,None,None,None),\
    (None,None,None,None),\
    (None,"Robert",None,2000),\
     (11,"Jack", None, 4100), \
    (12,"Steve", "Sales", None)  
  )
columns= ["id","employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.display()

# COMMAND ----------

df1=df.dropDuplicates(["id"])

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

help(df1.fillna(1))

# COMMAND ----------

df2.display()

# COMMAND ----------

df3=df2.fillna({"department":"Finance","Salary":4600})

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

df3.withColumn("Salary_Range",when(df3.salary<3000,"Low")
               .when(df3.salary>4000,"High")
               .otherwise("Medium")).display()

# COMMAND ----------


w=Window.partitionBy("DEPARTMENT").orderBy(col("salary").desc())
df3.withColumn("ranking",row_number().over(w)).display()
df3.withColumn("ranking",rank().over(w)).display()
df3.withColumn("ranking",dense_rank().over(w)).display()
