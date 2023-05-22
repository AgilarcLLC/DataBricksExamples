# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## When dropping tables you have to specify the drop in both the Delta space as well as the blob space.
# MAGIC
# MAGIC This is for Databricks managed tables only.

# COMMAND ----------

#Shows All managed user tables
dbutils.fs.ls('/user/hive/warehouse/')

# COMMAND ----------

# Example of the drop command.
# This drops the table from the blob storage but databricks still thinks it exsits and will show a large red error on the "Data" screen view
dbutils.fs.rm('dbfs:/user/hive/warehouse/forecast_prediction_647a13a3/', recurse=True)

# COMMAND ----------

# This command drops the table from the spark meta store
# Checking the "Data" tab screen you should now see the table completely removed
spark.sql("drop table if exists forecast_prediction_647a13a3")
