# Databricks notebook source
import os
import requests
import json
import urllib3

#Blotter data feed for month of May(05) With limit of 5 For testing
#url = 'https://data.wprdc.org/api/3/action/datastore_search?resource_id=1797ead8-8262-41cc-9099-cbc8a161924b&limit=5&q=2023-05'

#Blotter data feed for month of May(05) no limit
url = 'https://data.wprdc.org/api/3/action/datastore_search?resource_id=1797ead8-8262-41cc-9099-cbc8a161924b&q=2023-05'

http = urllib3.PoolManager()
#response = http.get('GET', url).json()['results'][0]
r = requests.get(url).json()['result']['records']
#r
police_data = spark.createDataFrame(json_normalize(r))
display(data)

# COMMAND ----------

target_table_name = "pittsburgh_police_blotter"
police_data.write.mode("overwrite").saveAsTable(target_table_name)
