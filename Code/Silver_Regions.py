# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format('parquet').load("abfss://bronze@databricks2706.dfs.core.windows.net/regions")
df.display()


# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Dropping _rescued_data**

# COMMAND ----------

df = df.drop ("_rescued_data")

# COMMAND ----------

df_silver_regions = df 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df_silver_regions.write.format('delta')\
                        .mode('overwrite')\
                        .save('abfss://silver@databricks2706.dfs.core.windows.net/regions')

# COMMAND ----------

