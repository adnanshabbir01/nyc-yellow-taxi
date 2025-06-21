# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistgacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistgacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistgacc.dfs.core.windows.net", "6d49b83c-da1d-44c1-b5b5-a249fd242d4b")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistgacc.dfs.core.windows.net", "yBa8Q~vuMnhk3HiXyXG8zPxkdTOw12y-Z8_1kbJy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistgacc.dfs.core.windows.net", "https://login.microsoftonline.com/ed570910-d325-4362-a77a-1441a44f0cf3/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading
# MAGIC

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@nyctaxistgacc.dfs.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC **Importing Libraries**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV Data**

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Type Data

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
    .option('inferSchema', True)\
    .option('header',True)\
    .load('abfss://bronze@nyctaxistgacc.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Zone Data

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
    .option('inferSchema', True)\
    .option('header',True)\
    .load('abfss://bronze@nyctaxistgacc.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data

# COMMAND ----------

schema_data = '''
       VendorID BIGINT,
       tpep_pickup_datetime TIMESTAMP,
       tpep_dropoff_datetime TIMESTAMP,
       passenger_count BIGINT,
       trip_distance DOUBLE,
       RatecodeID BIGINT,
       store_and_fwd_flag STRING,
       PULocationID BIGINT,
       DOLocationID BIGINT,
       payment_type BIGINT,
       fare_amount DOUBLE,
       extra DOUBLE,
       mta_tax DOUBLE,
       tip_amount DOUBLE,
       tolls_amount DOUBLE,
       improvement_surcharge DOUBLE, 
       total_amount DOUBLE,
       congestion_surcharge DOUBLE,
       Airport_fee DOUBLE
'''


# COMMAND ----------

df_trip = spark.read.format('parquet') \
    .schema(schema_data) \
    .option('recursiveFileLookup', True) \
    .load('abfss://bronze@nyctaxistgacc.dfs.core.windows.net/trips2024data/')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming a column

# COMMAND ----------

# MAGIC %md
# MAGIC **Taxi Trip type**

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('trip_type','trip_type_id')
df_trip_type = df_trip_type.withColumnRenamed('description','trip_type_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@nyctaxistgacc.dfs.core.windows.net/trip_type")\
    .save()

# COMMAND ----------

df_trip_zone.display() # We will break zone column becuase we have multiple zones in 1 column

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone1',split(col('Zone'),'/')[0])\
        .withColumn('zone2',split(col('Zone'),'/')[1])


df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@nyctaxistgacc.dfs.core.windows.net/trip_zone")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Data**

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date', to_date('tpep_pickup_datetime'))\
                 .withColumn('trip_month', month('tpep_pickup_datetime'))\
                 .withColumn('trip_year', year('tpep_pickup_datetime'))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# We are selecting just a few columns
df_trip = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount','trip_date','trip_month','trip_year')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
    .mode('append')\
    .option("path","abfss://silver@nyctaxistgacc.dfs.core.windows.net/trips2024data")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC