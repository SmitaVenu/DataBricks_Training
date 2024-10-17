# Databricks notebook source
nested_data='''{
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}'''

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

df=spark.read.json(spark.sparkContext.parallelize([nested_data]))

# COMMAND ----------

df.display()

# COMMAND ----------

batters_df = df.select(col("id"),col("type"), col("name"),col("ppu"), explode(col("batters.batter")).alias("batter"), explode(col("topping")).alias("topping"))
df2=df.withColumn("batter_1",explode("batters.batter")).withColumn("topping_1",explode("topping")).drop("batters","topping")



# COMMAND ----------

df_final=df2.withColumn("batter_id",col("batter_1.id")).withColumn("batter_type",col("batter_1.type")).withColumn("topping_id",col("topping_1.id")).withColumn("topping_type",col("topping_1.type")).drop("batter_1","topping_1")

# COMMAND ----------

df_final.display()
