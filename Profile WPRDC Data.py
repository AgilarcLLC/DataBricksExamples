# Databricks notebook source
pip install pandas_profiling

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd
from pandas_profiling import ProfileReport

#To use Profiler dataframe must be in Pandas Format. The toPandas function converts it from spark to pandas
testoutput = spark.read.table("default.silver_pittsburgh_rev_exp").toPandas()

df_profile = ProfileReport(testoutput, title="Pittsburgh Revenue and Spending", infer_dtypes=False)
profile_html = df_profile.to_html()
 
displayHTML(profile_html)
