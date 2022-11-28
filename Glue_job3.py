# SAP Project (Glue Job3)
# Read file from "Confirm zone" (.parquet)
# Transformation logic is applied as per Business Requirements on data from Confirm zone
# Save it to "Enrich Zone"   as csv or parquet format as per the requirement  

import pyspark
from pyspark.sql import *

import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_replace
import re


if __name__ == "__main__":

    print("========== Glue Job 3 ================")

    spark = SparkSession \
        .builder \
        .appName("SAP_proj_job3") \
        .master("local[2]") \
        .getOrCreate()
    

# read a parquet files from confirm zone

    df_parquet1= spark.read.parquet("s3://sap-spark-confirm-zone/output/new-data1-filtered.parquet")
    df_parquet2= spark.read.parquet("s3://sap-spark-confirm-zone/output/new-data2.parquet")
    
    # df1_r = df_parquet1.select(*(F.col(x).alias(x + '_df1') for x in df_parquet1.columns))
    # df2_r = df_parquet2.select(*(F.col(x).alias(x + '_df2') for x in df_parquet2.columns))

    print(df_parquet1.count())
    df_parquet1.show(3)
    print(df_parquet2.count())
    df_parquet2.show(2)

    print("======================================================")
    
    df_parquet1.select("country").show(5, truncate=False)
    
    print("------------------------------------------------------------")
    
    df_parquet2.select("county").show(5, truncate=False)
    
    print("------------------------------------------------------------")
    
# join two DF


    df_parquet = df_parquet1.join(df_parquet2, df_parquet1.country ==  df_parquet2.county, "inner") \
        .drop(df_parquet2.region).drop(df_parquet2.county) \
        .drop(df_parquet2.order_id).drop(df_parquet2.ship_date).drop(df_parquet2.order_date)
    # # df_join = df1_r.join(df2_r,df1_r.country_df1 ==  df2_r.county_df2,"inner")
    
    print(df_parquet.count())
    df_parquet.show(5)

    # df_parquet.write \
    #     .format("csv") \
    #     .mode("overwrite") \
    #     .option("header", True ) \
    #     .option("path","s3://sap-spark-enrich-zone/output/new-data-join.csv") \
    #     .save()
        
        
    df_parquet.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path","s3://sap-spark-enrich-zone/output/new-data-join") \
        .save()
    
    print("================ Done =====================")