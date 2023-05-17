# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Showcases OpenAIs ability to parse out relevent information from an entire PDF page.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Adding this command cell just incase user is missing openai python library.

# COMMAND ----------

pip install openai

# COMMAND ----------

pip install pdfplumber

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### We read directly from the PDF that was imported to Databricks
# MAGIC
# MAGIC File Location
# MAGIC
# MAGIC /Workspace/Users/chhirsh@gmail.com/Alimak-Scando-650-Brochure-w-max-lift-logo.pdf
# MAGIC
# MAGIC ### Script searches through entire PDF marking any pages that contain the words "Technical specifications"

# COMMAND ----------

import os
import openai
import pdfplumber
import re
import pandas as pd

openai.api_key = "sk-uCplXmEuLWmxxxxxxxxxxxxxZqpcU7hXqhgWBbLaBj"
results = []

#Open File an search for "Technical specifications"

pdf_file = "/Workspace/Users/chhirsh@gmail.com/Alimak-Scando-650-Brochure-w-max-lift-logo.pdf"
pattern = re.compile(r"Technical specifications", flags=re.IGNORECASE)
with pdfplumber.open(pdf_file) as pdf:
    pages = pdf.pages
    for page_nr, pg in enumerate(pages, 0):
        content = pg.extract_text()
        for match in pattern.finditer(content):
            print('Keyword:',match.group(), '|  Page:', page_nr, '| Location: ',content.index(match.group()))
            foundPageNum = page_nr

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Show the Text extracted from the PDF

# COMMAND ----------

table_page = pdf.pages[foundPageNum]
testouput01 = table_page.extract_text_simple(x_tolerance=3, y_tolerance=3)

testouput01

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Using the example text from the Alimak Scando 650 brocure, AI will will read through and extract the relevant info requested.
# MAGIC
# MAGIC the response from the AI returned in  dilimeted format, with the "split" function we are able to write each line of the response to a point in a python array.

# COMMAND ----------

response = openai.Completion.create(
  model="text-davinci-003",
  prompt=f"Please extract all mechanical measurment contianing numbers into the table format [section | Spec] from the input text.\n\n\"\"\"{page_text}\"\"\"",
  temperature=0.7,
  max_tokens=3000,
  top_p=1,
  frequency_penalty=0,
  presence_penalty=0
)

output = response.choices[0].text.split('\n')
output

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### After the output format is converted to an array we can then piviot that array into a data table to easily view.

# COMMAND ----------

for row in output:
    temp = row.split('|')
    results.append(temp)

output_df = pd.DataFrame(results,columns = ['Section', 'Spec'])

# Add adtional columns with plan to expand more makes and models
output_df['Make'] = 'Alimak'
output_df['Model'] = 'Scando 650'

display(output_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write the data to a Delta Table

# COMMAND ----------

data = spark.createDataFrame(output_df)

data.write.mode("overwrite").saveAsTable("hive_metastore.default.bronze_heavy_machinery")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Verify the Table was created

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM default.bronze_heavy_machinery;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Clean Up the table using SQL and create silver level

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE hive_metastore.default.silver_heavy_machinery AS (
# MAGIC
# MAGIC SELECT * FROM default.bronze_heavy_machinery
# MAGIC WHERE 
# MAGIC Section NOT IN ('','null','Section ')
# MAGIC OR
# MAGIC Spec NOT IN ('','null',' Spec')
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Display created silver level table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM default.silver_heavy_machinery;
