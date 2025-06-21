# Databricks notebook source
# MAGIC %md
# MAGIC Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistgacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistgacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistgacc.dfs.core.windows.net", "6d49b83c-da1d-44c1-b5b5-a249fd242d4b")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistgacc.dfs.core.windows.net", "yBa8Q~vuMnhk3HiXyXG8zPxkdTOw12y-Z8_1kbJy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistgacc.dfs.core.windows.net", "https://login.microsoftonline.com/ed570910-d325-4362-a77a-1441a44f0cf3/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading, Data Writing and Creating Delta Tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC Zone Data

# COMMAND ----------

df_zone = spark.read.format('parquet')\
      .option('inferSchema',True)\
      .option('header',True)\
      .load('abfss://silver@nyctaxistgacc.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta')\
    .mode('append')\
    .save('abfss://gold@nyctaxistgacc.dfs.core.windows.net/trip_zone')

df_zone.write.format('delta')\
    .mode('append')\
    .saveAsTable('gold.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Type Data

# COMMAND ----------

df_type = spark.read.format('parquet')\
      .option('inferSchema',True)\
      .option('header',True)\
      .load('abfss://silver@nyctaxistgacc.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_type.display()

# COMMAND ----------

df_type.write.format('delta')\
    .mode('overwrite')\
    .save('abfss://gold@nyctaxistgacc.dfs.core.windows.net/trip_type')

df_type.write.format('delta')\
    .mode('overwrite')\
    .saveAsTable('gold.trip_type')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data

# COMMAND ----------

df_trip = spark.read.format('parquet')\
    .option('inferSchema',True)\
    .option('header',True)\
    .load('abfss://silver@nyctaxistgacc.dfs.core.windows.net/trips2024data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta')\
    .mode('append')\
    .save('abfss://gold@nyctaxistgacc.dfs.core.windows.net/trips2024data')

df_trip.write.format('delta')\
    .mode('append')\
    .saveAsTable('gold.trip_trip')

# COMMAND ----------

df_trip.display()