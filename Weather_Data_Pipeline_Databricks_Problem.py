# Databricks notebook source
# MAGIC %md
# MAGIC # 🌦️ Simple Data Pipeline with Databricks: Weather API to Delta Lake

# COMMAND ----------

import requests
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Fetch data from the Open-Meteo API

# COMMAND ----------

# Replace with your desired coordinates
latitude = 40.71  # Latitude for the location
longitude = -74.01  # Longitude for the location

#This longitude and latitude is for New York

# Construct the API URL with the specified coordinates and request hourly temperature data
url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m"

print('url when formatted is ', url)


# COMMAND ----------

# Send a GET request to the API


# Parse the JSON response from the API

# Print the parsed JSON response
print(data)


# COMMAND ----------

#print schema of data
print(data.keys())

# COMMAND ----------

# Pretty-print the entire dictionary
print(json.dumps(data, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Note:** In the real world, I would dump this data into the Landing layer as a JSON file, following the medallion architecture. Then read it back in for further transformation

# COMMAND ----------

# DBTITLE 1,Real world
#write this json dump to a json file in this location /Volumes/test/testschema/volume


# COMMAND ----------

#read the json file and load it into a data object


# COMMAND ----------


# Extract the hourly data from the JSON response


# COMMAND ----------

# DBTITLE 1,Easy way to convert to Spark Dataframe
#convert pandas dataframe to spark dataframe



# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Perform transformation by Add Metadata and convert temperature to Farenheit

# COMMAND ----------



# Add ingestion timestamp
spark_df = spark_df.withColumn("ingestion_date", current_timestamp())

#Add Farheint temperature


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to Delta Table

# COMMAND ----------

# Write spark_df to delta table with schema overwrite


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Read from Delta Table and Query

# COMMAND ----------

# Read the Delta table
df_read = spark.read.table('weather_data')
df_read.createOrReplaceTempView("weather_data")

# Example query
spark.sql("""
SELECT date(time) as day, AVG(temperature_2m) as avg_temp
FROM weather_data
GROUP BY day
ORDER BY day
""").display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_data