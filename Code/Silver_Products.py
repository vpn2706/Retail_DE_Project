# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@databricks2706.dfs.core.windows.net/products')
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Dropping _rescued_data**

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Understanding the data**

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.describe().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Checking Nulls**

# COMMAND ----------

df.select([count(c).alias(c) for c in df.columns]).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC This shows that there are no nulls present in the dataset

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Using Functions to Apply a 10% Discount on Products**

# COMMAND ----------

df.createOrReplaceTempView('products')

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE FUNCTION databricks_catalog.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE 
# MAGIC LANGUAGE SQL 
# MAGIC RETURN p_price * 0.90

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT brand, price,ROUND(databricks_catalog.bronze.discount_func(price),2)AS discounted_price
# MAGIC FROM products

# COMMAND ----------

# MAGIC %md 
# MAGIC Saving it to the df

# COMMAND ----------

df_silver_products = df.withColumn('discounted_price', round(expr('databricks_catalog.bronze.discount_func(price)'),2))
df_silver_products.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Applying price-based transformations to derive analytical insights**
# MAGIC

# COMMAND ----------

quantiles = df_silver_products.approxQuantile('price', [0.33,0.66],0)
low = quantiles[0]
high = quantiles[1]


# COMMAND ----------

df_silver_products = df_silver_products.withColumn('price_category', when(col("price")< low, 'Low')\
                                                    .when(col("price")< high, 'Medium')\
                                                        .otherwise('High'))
df_silver_products.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Identifying high-value (expensive) products**

# COMMAND ----------

q3 = df_silver_products.approxQuantile('price', [0.75], 0)[0]

df_silver_products = df_silver_products.withColumn('is_expensive', when(col('price')>=q3, 'Yes')\
                                                                    .otherwise('No'))
df_silver_products.display()


# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Writing**

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

target_path = "abfss://silver@databricks2706.dfs.core.windows.net/products"

try:
    existing_df = spark.read.format("delta").load(target_path).select("product_id")
    table_exists = True
except:
    table_exists = False

if table_exists:
    #  Remove already existing records
    df_silver_products = df_silver_products.join(
        existing_df,
        on="product_id",
        how="left_anti"
    )

# ✅Append ONLY new data
df_silver_products.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(target_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating a table 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.products 
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@databricks2706.dfs.core.windows.net/products'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_catalog.silver.products