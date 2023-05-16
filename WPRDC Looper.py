# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Pittsburgh Data Downloader
# MAGIC
# MAGIC ### Purpose: 
# MAGIC Pulls data from a given WPRDC resource and download
# MAGIC
# MAGIC ### Dev Notes
# MAGIC API only allows for a max of 50,000 rows through API request. for loop used to gather all rows.

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

url = 'https://data.wprdc.org/api/3/action/datastore_search?offset=0&limit=1&resource_id=f61f6e8c-7b93-4df3-9935-4937899901c7'

http = urllib3.PoolManager()
row_count = int(requests.get(url).json()['result']['total'])
segments_int = math.floor(row_count / 10000)
segments_dec = row_count / 10000

#total: 617753
#for loop does chunks of 10000

#segments_int
for i in range(segments_int + 1):
    i = i * 10000
    r = requests.get(f'https://data.wprdc.org/api/3/action/datastore_search?offset={i}&limit=10000&resource_id=f61f6e8c-7b93-4df3-9935-4937899901c7').json()['result']['records']
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