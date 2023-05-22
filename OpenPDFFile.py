# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Test to Connect to PDF File within databricks Workspace
# MAGIC
# MAGIC Databricks allows you  to import .csv and JSON directly to delta tables. However, this is a test to find and read a file that is stored on databricks outside of the delta enviorment

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Users are able to upload/import any kind of files to the "Workspace".
# MAGIC
# MAGIC I have to figure out the true path of those files.

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls("dbfs:/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Using the Shell magic command I can search the entire Databrick root to narrow where the file is stored
# MAGIC
# MAGIC The sh command below lists all files in the directory. This works for both Repos and Workspace files

# COMMAND ----------

# MAGIC %sh ls /Workspace/Users/chhirsh@gmail.com/
