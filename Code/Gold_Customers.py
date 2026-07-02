# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *
from delta.tables import DeltaTable 


# COMMAND ----------

# DBTITLE 1,Cell 2
dbutils.widgets.text("init_load_flag", "0")
init_load_flag = int(dbutils.widgets.get('init_load_flag'))


# COMMAND ----------

if not spark.catalog.tableExists("databricks_catalog.gold.Dimcustomers"):
    init_load_flag = 1
else:
    init_load_flag = 0

print("init_load_flag:", init_load_flag)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Data reading from the source**

# COMMAND ----------

df = spark.sql('select * from databricks_catalog.silver.customers')

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Dividing Old vs New**

# COMMAND ----------

if init_load_flag ==0:

    df_old = spark.sql('select DimCustomerKey, customer_id, create_date, update_date from databricks_catalog.gold.Dimcustomers')
else:
    df_old = spark.sql('select 0 DimCustomerKey, 0 customer_id, 0 created_date, 0 updated_date from databricks_catalog.silver.customers where 1=0')

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Renaming old columns**

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey", 'old_DimCustomerKey')\
               .withColumnRenamed('customer_id', 'old_customer_id')\
               .withColumnRenamed('created_date', 'old_created_date')\
               .withColumnRenamed("updated_date", 'old_updated_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying Join with the old records**

# COMMAND ----------

df_join = df.join(df_old, df['customer_id'] == df_old['old_customer_id'], 'left')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Seprating New vs Old Records**

# COMMAND ----------

df_new = df_join.filter(df_join['old_customer_id'].isNull())

# COMMAND ----------

df_old = df_join.filter(df_join['old_customer_id'].isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_old 

# COMMAND ----------

#Dopping columns which are not required 
df_old = df_old.drop("old_customer_id")

#Renaming old_DimCustomerKey to DimCustomerKey
df_old = df_old.withColumnRenamed('old_DimCustomerKey', 'DimCustomerKey')

#Renaming old_created_date & 
df_old =df_old.withColumnRenamed("old_created_date", 'create_date')
df_old = df_old.withColumn('create_date', to_timestamp(col('create_date')))

#Renaming and updating the old_updated_date
df_old = df_old.withColumnRenamed("old_updated_date", 'update_date')
df_old = df_old.withColumn('update_date', current_timestamp())

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_new 

# COMMAND ----------

#Dopping columns which are not required 
df_new = df_new.drop("old_customer_id",'old_created_date', 'old_updated_date')


#Recreating and updating the create_date and update_date
df_new = df_new.withColumnRenamed('old_DimCustomerKey', 'DimCustomerKey')
df_new = df_new.withColumn('create_date', current_timestamp())
df_new = df_new.withColumn('update_date', current_timestamp())

df_new.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Adding surrogate key**

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0 
else:
    df_maxsur = spark.sql('select max(DimCustomerKey) as max_surrogate_key from databricks_catalog.gold.Dimcustomers')
    #Converting max_sur to max_surrogate_key
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey', lit(max_surrogate_key)+col('DimCustomerKey'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### **Creating a dimension key**

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey', monotonically_increasing_id()+1)


# COMMAND ----------


df_new.limit(10).display()

# COMMAND ----------

df_new = df_new.withColumn('DimCustomerKey', lit(max_surrogate_key)+col('DimCustomerKey'))
df_new.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Union of df_old and df_new** 

# COMMAND ----------

df_final = df_new.unionByName(df_old)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **SCD TYPE-1** 

# COMMAND ----------


# Cast column to BIGINT
df_fixed = df_final.withColumn(
    "DimCustomerKey",
    col("DimCustomerKey").cast("bigint")
)

# Overwrite table with new schema
df_fixed.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("databricks_catalog.gold.Dimcustomers")

# COMMAND ----------

if (spark.catalog.tableExists('databricks_catalog.gold.Dimcustomers')):
    delta_obj = DeltaTable.forPath(spark, 'abfss://gold@databricks2706.dfs.core.windows.net/Dimcustomers')

    delta_obj.alias('trg').merge(df_fixed.alias('src').dropDuplicates(['DimCustomerKey']), 'trg.DimCustomerKey = src.DimCustomerKey')\
                      .whenMatchedUpdateAll()\
                      .whenNotMatchedInsertAll()\
                      .execute()

    

else:
    df_fixed.write.mode('overwrite')\
    .format('delta')\
    .option('path', 'abfss://gold@databricks2706.dfs.core.windows.net/Dimcustomers')\
    .saveAsTable('databricks_catalog.gold.Dimcustomers')
    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM databricks_catalog.gold.dimcustomers

# COMMAND ----------

