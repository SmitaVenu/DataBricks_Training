# Databricks notebook source
from pyspark.sql.functions import *
input_path="/Volumes/databricks_training_smita/default/raw/"

# COMMAND ----------

def add_injestion(df):
    df1=df.withColumn("injestion_date",current_timestamp())
    return df1
