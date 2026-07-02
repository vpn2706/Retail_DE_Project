# Databricks notebook source
# MAGIC %md 
# MAGIC ##**Fact Orders**

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Data Reading

# COMMAND ----------

df = spark.sql('SELECT * FROM databricks_catalog.silver.orders')

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

df_dimcus = spark.sql('SELECT DimCustomerKey, customer_id As dim_customer_id FROM databricks_catalog.gold.dimcustomers')

df_dimpro = spark.sql('SELECT product_id AS DimProductKey, product_id As dim_product_id FROM databricks_catalog.gold.dimproducts')



# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Fact DataFrame** 

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id']==df_dimcus['dim_customer_id'], how="left").join(df_dimpro, df['product_id']==df_dimpro['dim_product_id'], how="left")

# COMMAND ----------

df_dimcategory =df_fact.select('order_category')\
    .distinct()\
    .withColumn('DimCategoryKey', monotonically_increasing_id()+1)
    

# COMMAND ----------

df_dimcategory = df_dimcategory.withColumn('order_category', when(col('order_category').isNull(), lit('0')).otherwise(col('order_category')))


# COMMAND ----------

df_fact_new = df_fact.dropDuplicates(["order_id"])
df_fact_new = df_fact.join(df_dimcategory, on="order_category", how="left")
df_fact_new = df_fact_new.drop(
    'dim_customer_id',
    'dim_product_id',
    'year',
    'month',
    'quarter',
    'order_category',
    'order_value_rank_year',
    'customer_id',
    'product_id'
     
     )

# COMMAND ----------

df_fact_new.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Upsert on Fact table**
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('databricks_catalog.gold.FactOrders'):
    dlt_object = DeltaTable.forName(spark, 'databricks_catalog.gold.FactOrders')
    dlt_object.alias('trg').merge(df_fact_new.alias('src').dropDuplicates(['order_id']), 'trg.order_id =src.order_id')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:
    df_fact_new.write.format('delta')\
        .option('path', 'abfss://gold@databricks2706.dfs.core.windows.net/FactOrders')\
        .saveAsTable('databricks_catalog.gold.FactOrders')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_catalog.gold.factorders

# COMMAND ----------

