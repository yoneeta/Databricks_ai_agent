# Databricks notebook source
import pandas as pd

pdDF = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vTR1rnb6hB0K41k_HgvMYlJkjlbo20VISVeDIOhYc0lfZvBwkHfLc7Y30YVwKCPryTZj96SE_lNY3jz/pub?gid=1010722291&single=true&output=csv")
#Convert to spark DF

sparkDF = spark.createDataFrame(pdDF)

# COMMAND ----------

print(sparkDF)

# COMMAND ----------

display(pdDF)

# COMMAND ----------

sparkDF.write.mode("overwrite").saveAsTable("default.investment_table")