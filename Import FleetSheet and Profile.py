# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Script to ingest and profile the FleetSheet document

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Start by dropping bronze ingest layer if it already exists

# COMMAND ----------

#target Table location

table_schema = 'hive_metastore.default.'
table_name = 'bronze_fleet_sheet'

#Drop Current table
dbutils.fs.rm('dbfs:/user/hive/warehouse/' + table_name + '/', recurse=True)
spark.sql("drop table if exists " + table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read the .csv that was manually uploaded into the "Workspace" by the user

# COMMAND ----------

raw_fleet_sheet = (spark.read
  .format("csv")
  .option("header", "true")
  .load("file:/Workspace/Users/chhirsh@gmail.com/AprilFleetSheet.csv")
)

#display(raw_fleet_sheet)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Delta Table

# COMMAND ----------

#Create Table

raw_fleet_sheet.write.mode("overwrite").saveAsTable(table_schema + table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Verify the Table was created and is populated

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM default.bronze_fleet_sheet;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Install Pandas profiling library

# COMMAND ----------

pip install pandas_profiling

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Initiate The profiler
# MAGIC
# MAGIC This will take some time to complete because we are ignoring datatypes and the dataset has 100+ columns

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd
from pandas_profiling import ProfileReport

#To use Profiler dataframe must be in Pandas Format. The toPandas function converts it from spark to pandas
testoutput = spark.read.table("default.bronze_fleet_sheet").toPandas()

df_profile = ProfileReport(testoutput, title="FleetSheet Profiling Report", infer_dtypes=False)
profile_html = df_profile.to_html()
 
displayHTML(profile_html)
