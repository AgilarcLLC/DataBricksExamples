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

import os
import openai
import pandas as pd

openai.api_key = "sk-uCplXmEuLWmxxxxxxxxxxxxxxxxpcU7hXqhgWBbLaBj"
results = []

page_text = 'ALIMAK SCANDO 650\nMain characteristics\n• Robust design and excellent comfort \n• Modular car design with extensions and multiple\ndoor and ramp options\n• Single and dual car configurations \n• Triple entrance possible at ground level and at any\nlanding level\n• A high-efficiency helical gearbox provides lower\npower consumption and reduced operational costs\n• ALC-II collective control system with group control\nand internal fault diagnostics system. Control panel\nmovable between two sides \n• Built-in stainless steel electrical cabinet maximises\ninternal hoist dimensions for increased lifting\ncapacity \n• Wide range of optional equipment and functions\n• Remote Monitoring System — A3\n• Fully compliant with EN, ANSI, AS, PB and the\nmajority of all national regulations\nTECHNICAL SPECIFICATIONS\nALIMAK SCANDO 650 DOL FC\nMotor control Direct-on-line (DOL) Frequency control (FC)\nMax. payload capacity 1,500–3,200 kg (3,300–7,054 lbs) * 1,500–3,200 kg (3,300–7,054 lbs) *\nMax. travelling speed 38 m/min. (125 fpm) 66 m/min.(216 fpm)\nMax. lifting height 250 / 400 m (825 / 1300 ft) * 250 / 400 m (825 / 1,300 ft) *\nCar width, internal 1.5 m (4’-11”) 1.5 m (4’-11”)\nCar length, internal 2.8–5.0 m (9\'- 2 1⁄4"-16\'- 4 3⁄4") 2.8–5.0 m (9\'- 2 1⁄4"-16\'- 4 3⁄4")\nCar height, internal 2.3 m (7’-6 1/2”) 2.3 m (7’-6 1/2”)\nMax. load space per car 17 m3(610 ft3) 17 m3(610 ft3)\nNo. of motors 2 x 11 kW 2 or 3 x 11 kW\nSafety device type ALIMAK GFD  ALIMAK GFD\nPower supply range 400–500 V, 50 or 60 Hz, 3 phase  400–500 V, 50 or 60 Hz, 3 phase\nFuse ratings From 60 A From 63 A \nType of mast 650, tubular steel with integrated rack 650, tubular steel with integrated rack\nLength mast section 1.508 m (4’-11 3⁄8”) 1.508 m (4’-11 3⁄8”)\nWeight mast section with 1 rack 118 kg (260 lbs) 118 kg (260 lbs)\nRack module 5  5 \n* Increased payload capacity and lifting height and on request.\nFor other demands or specifications, please consult your Alimak representative.\n9\n1\n0\nwww.alimak.com 2\nb. \ne\nF\nN/\nE\n7 \nPictures are illustrative only and do not necessarily show the configuration of products on the market at a given point in time. Products must be used in conformity with safe practice and applicable 21\nstatutes, regulations, codes and ordinances. Specifications of products and equipment shown herein are subject to change without notice. Copyright © 2019 Alimak Group. All rights reserved.  1\nAlimak and Scando are registered trademarks of Alimak Group. '

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
