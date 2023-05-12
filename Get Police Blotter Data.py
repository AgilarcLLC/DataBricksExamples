# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream data from data source

# COMMAND ----------

import os
import requests
import json
import urllib3
import pandas as pd


#Blotter data feed for month of May(05) With limit of 5 For testing
#url = 'https://data.wprdc.org/api/3/action/datastore_search?resource_id=1797ead8-8262-41cc-9099-cbc8a161924b&limit=5&q=2023-05'

#Blotter data feed for month of May(05) no limit
#url = 'https://data.wprdc.org/api/3/action/datastore_search?resource_id=1797ead8-8262-41cc-9099-cbc8a161924b&q=2023-05'

#Entire Blotter dataset
url = 'https://data.wprdc.org/api/3/action/datastore_search?resource_id=1797ead8-8262-41cc-9099-cbc8a161924b'

http = urllib3.PoolManager()
#response = http.get('GET', url).json()['results'][0]
r = requests.get(url).json()['result']['records']
#r
police_data = spark.createDataFrame(pd.json_normalize(r))
display(police_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert the data from the blotter API to a Delta Table

# COMMAND ----------

target_table_name = "pittsburgh_police_blotter"
police_data.write.mode("overwrite").saveAsTable("hive_metastore.default.bronze_pittsburgh_police_blotter")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix the datatypes on the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE hive_metastore.default.silver_pittsburgh_police_blotter AS (
# MAGIC SELECT
# MAGIC CAST(INCIDENTTIME AS DATE) AS INCIDENTTIME,
# MAGIC CAST(COUNCIL_DISTRICT AS INT) AS COUNCIL_DISTRICT,
# MAGIC CLEAREDFLAG,
# MAGIC CAST(HIERARCHY AS INT) AS HIERARCHY, 
# MAGIC INCIDENTNEIGHBORHOOD, 
# MAGIC CAST(INCIDENTZONE AS INT) AS INCIDENTZONEAS, 
# MAGIC CAST(INCIDENTTRACT AS INT) AS INCIDENTTRACTAS, 
# MAGIC CAST(CCR AS BIGINT) AS CCR, 
# MAGIC INCIDENTHIERARCHYDESC, 
# MAGIC CAST(PUBLIC_WORKS_DIVISION AS INT) AS PUBLIC_WORKS_DIVISION, 
# MAGIC X, 
# MAGIC INCIDENTLOCATION, 
# MAGIC Y, 
# MAGIC CAST(PK AS BIGINT) AS PK, 
# MAGIC CAST(`_id` AS BIGINT) AS `_id`, 
# MAGIC OFFENSES
# MAGIC
# MAGIC FROM hive_metastore.default.bronze_pittsburgh_police_blotter);
