# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic Capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name", '')

# COMMAND ----------

file_name = dbutils.widgets.get('file_name')

# COMMAND ----------

file_name

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format('parquet').load(f'abfss://source@databricks2706.dfs.core.windows.net/{file_name}')
df.display()

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@databricks2706.dfs.core.windows.net/checkpoint_{file_name}")\
    .load(f"abfss://source@databricks2706.dfs.core.windows.net/{file_name}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode('append')\
    .option('checkpointLocation', f'abfss://bronze@databricks2706.dfs.core.windows.net/checkpoint_{file_name}')\
    .option('path', f'abfss://bronze@databricks2706.dfs.core.windows.net/{file_name}')\
    .trigger(once=True)\
    .start()

# COMMAND ----------

