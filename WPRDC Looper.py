# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Pittsburgh Data Downloader
# MAGIC
# MAGIC ### Purpose: 
# MAGIC Pulls data from a given WPRDC (Pittsburgh Open Data) resource and download https://data.wprdc.org/dataset/city-revenues-and-expenses
# MAGIC
# MAGIC ### Dev Notes
# MAGIC API only allows for a max of 50,000 rows through API request. for loop used to gather all rows.
# MAGIC
# MAGIC This should also work for any other WPRDC resources

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Program loops through 10,000 API rows at a time and writes them to an array.

# COMMAND ----------

import os
import math
import requests
import json
import urllib3
import pandas as pd

results = []

resource_id = 'f61f6e8c-7b93-4df3-9935-4937899901c7'

url = f'https://data.wprdc.org/api/3/action/datastore_search?offset=0&limit=1&resource_id={resource_id}'

http = urllib3.PoolManager()
row_count = int(requests.get(url).json()['result']['total'])
segments_int = math.floor(row_count / 10000)
segments_dec = row_count / 10000

#total: 617753
#for loop does chunks of 10000

#segments_int
for i in range(segments_int + 1):
    i = i * 10000
    r = requests.get(f'https://data.wprdc.org/api/3/action/datastore_search?offset={i}&limit=10000&resource_id={resource_id}').json()['result']['records']
    for x in r:
        results.append(x)

data = spark.createDataFrame(pd.json_normalize(results))
#display(data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Array is converted to a Dataframe and then written as a Delta Table

# COMMAND ----------

data.write.mode("overwrite").saveAsTable("hive_metastore.default.bronze_pittsburgh_rev_exp")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query the newly table can now be queried

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) FROM default.bronze_pittsburgh_rev_exp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### The returned SQL count should match the number below proving all avalible records were downloaded
# MAGIC
# MAGIC row_count is defined in the the python above from the initial API request.

# COMMAND ----------

display(row_count)


df = spark.table('defaul.bronze_pittsburgh_rev_exp')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### This SQL converts the data in the delta table to the correct datatype and creates a silver level table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE hive_metastore.default.silver_pittsburgh_rev_exp AS (
# MAGIC
# MAGIC SELECT
# MAGIC CAST(fund_number AS INT) AS fund_number,
# MAGIC CAST(general_ledger_date AS DATE) AS general_ledger_date,
# MAGIC CAST(cost_center_number AS INT) AS cost_center_number,
# MAGIC object_account_description,
# MAGIC cost_center_description,
# MAGIC department_name,
# MAGIC CAST(ledger_code AS INT) AS ledger_code,
# MAGIC CAST(amount AS DOUBLE) AS amount,
# MAGIC CAST(object_account_number AS INT) AS object_account_number,
# MAGIC ledger_descrpition,
# MAGIC CAST(`_id` AS BIGINT) AS id_pk,
# MAGIC fund_description,
# MAGIC CAST(department_number AS INT) AS department_number
# MAGIC FROM default.bronze_pittsburgh_rev_exp
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Display created table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM default.silver_pittsburgh_rev_exp;
