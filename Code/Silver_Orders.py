# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Reading**

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.window import Window 

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://bronze@databricks2706.dfs.core.windows.net/orders')
df.display()

# COMMAND ----------


df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### **Understanding Data**

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
# MAGIC No null values were found across all columns

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Dropping _rescued_data (This come from the autoloader)**
# MAGIC

# COMMAND ----------

df = df.drop('_rescued_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Unit Price**

# COMMAND ----------

df_silver_orders = df.withColumn('unit_price', round(col('total_amount')/ col('quantity'),2))
df_silver_orders.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Time Intelligence**

# COMMAND ----------

df_silver_orders = df.withColumn('year', year(col("order_date")))\
                     .withColumn('month', month(col("order_date")))\
                     .withColumn('quarter', quarter(col("order_date")))\
                     .withColumn('month_name', date_format(col('order_date'), 'MMM'))
                     
df_silver_orders.display()
    

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Order Value Segmentation** 

# COMMAND ----------

# MAGIC %md
# MAGIC Order values were segmented using percentile-based bucketing (33rd and 66th percentiles) to ensure balanced and data-driven categorization.

# COMMAND ----------

quantiles = df.approxQuantile("total_amount", [0.33, 0.66], 0)
low = quantiles[0]
high = quantiles[1]
df_silver_orders = df_silver_orders.withColumn('order_category', when(col('total_amount')< low, 'Low')\
                                                                .when(col('total_amount')<high, 'Medium')\
                                                                    .otherwise('High'))
df_silver_orders.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Ranking Feature Creation**

# COMMAND ----------

# MAGIC %md 
# MAGIC A dense ranking feature was created based on total order amount, partitioned by year, to enable year-wise performance analysis. This transformation helps identify top-performing orders within each year
# MAGIC

# COMMAND ----------

df_silver_orders = df_silver_orders.withColumn('order_value_rank_year', dense_rank().over(Window.partitionBy('year').orderBy(desc('total_amount'))))
df_silver_orders.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data Writing**

# COMMAND ----------

df_silver_orders.write.format('delta').mode('overwrite').save('abfss://silver@databricks2706.dfs.core.windows.net/orders')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Creating Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_catalog.silver.orders
# MAGIC USING DELTA 
# MAGIC LOCATION 'abfss://silver@databricks2706.dfs.core.windows.net/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_catalog.silver.orders

# COMMAND ----------

