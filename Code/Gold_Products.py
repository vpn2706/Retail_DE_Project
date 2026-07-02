import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *
my_rules = {
    'rule1': 'product_id IS NOT NULL',
    'rule2':'product_name IS NOT NULL'

}

@dlt.expect_all_or_drop(my_rules)
@dlt.table
def DimProducts_stage():
    df = spark.readStream.table('databricks_catalog.silver.products')\
                        .withColumn('updated_at', current_timestamp())
    return df 


@dlt.view 
def DimProducts_view():
    df = spark.readStream.table('Live.Dimproducts_stage')
    return df 

dlt.create_streaming_table('DimProducts')

dlt.apply_changes(
    
    target = "DimProducts",
    source = "Live.DimProducts_view",
    keys = ["product_id"],
    sequence_by =("updated_at"),
    stored_as_scd_type = 2

)
