# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Reading** 

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@databricks2706.dfs.core.windows.net/customers')
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Understanding Data** 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Checking Null values**

# COMMAND ----------

df.select([count(c).alias(c) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC No null values found 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dropping _rescued_data**

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dropping Duplicate Customer ids**

# COMMAND ----------

df_silver_customers = df.dropDuplicates(['customer_id'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Standardize Casing**

# COMMAND ----------

df_silver_customers = df.withColumn('first_name', initcap(col("first_name")))\
                        .withColumn('last_name', initcap(col('last_name')))\
                        .withColumn('email', lower(col('email')))\
                        .withColumn('city', initcap(col("city")))\
                        .withColumn('state', initcap(col("state")))

                    

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Validate email format**

# COMMAND ----------

df_silver_customers = df.filter(col("email").contains('@'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Extracting domains from an email**

# COMMAND ----------

df_silver_customers = df_silver_customers.withColumn('domains', split(col('email'),'@')[1])


# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Creating full_name** 

# COMMAND ----------

df_silver_customers = df_silver_customers.withColumn('full_name', concat(col("first_name"), lit(' '), col("last_name")))



# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Reordering columns** 

# COMMAND ----------

df_silver_customers = df_silver_customers.select(
    'customer_id', 
    "first_name",
    'last_name', 
    'full_name',
    'email',
    'domains',
    'city',
    'state'

)
df_silver_customers.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Writing**

# COMMAND ----------

df_silver_customers.write.format('delta')\
                         .mode('overwrite')\
                         .option('mergeSchema', 'true')\
                         .save('abfss://silver@databricks2706.dfs.core.windows.net/customers')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Creating a table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA if NOT EXISTS databricks_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.customers
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@databricks2706.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_catalog.silver.customers

# COMMAND ----------

