# SAP Project (Glue Job2)
# Read file from "Transform zone" (.parquet)
# Filter Application, Type Casting, Column Renaming, Sorting Data is done on data from Transform zone
# Save it to "Confirm Zone" as parqurt   

import pyspark
from pyspark.sql import *

# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job

if __name__ == "__main__":

    print("========== Glue Job 2 ================")

    spark = SparkSession \
        .builder \
        .appName("SAP_proj_job2") \
        .master("local[2]") \
        .getOrCreate()
    
#------------------ for creating dynamic dataframe from catlog table (table created by crawler)-----------------    
    # glueContext = GlueContext(SparkContext.getOrCreate())
    
    # dynamicframe_catlog = glueContext.create_dynamic_frame.from_catalog(
    #     database = "sap-raw-db",
    #     table_name = "2_sales_records_csv")
        
    # dynamicframe_catlog.printSchema()
    # dynamicframe_catlog.show(5)
    
#------------------ for converting dynamic dataframe into spark dataframe----------------------
    # dynamicframe_catlog.toDF().show(5)
    
    

#1. read parquet files
    df_parquet= spark.read.parquet("s3://sap-spark-transform-zone/output/new-data1.parquet")
    
    df_parquet2= spark.read.parquet("s3://sap-spark-transform-zone/output/new-data2.parquet")
    
    
    
    
    # df_parquet.show(5)
    print("No of rows original = ", df_parquet.count())
    print(df_parquet.columns)
    print(type(df_parquet.columns))

    df_parquet = df_parquet.filter(df_parquet.country == "Israel")
    # df_parquet.show(10)
    print("No of rows filtered = ", df_parquet.count())
    print(df_parquet.distinct().count())

    df_offline = df_parquet.filter(df_parquet.sales_channel == "Offline")
    # df_offline.show()
    print("No of rows filtered on sales channel = ", df_offline.count())

    df_offline.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path","s3://sap-spark-confirm-zone/output/new-data1-filtered.parquet") \
        .save()
        
    df_parquet2.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path","s3://sap-spark-confirm-zone/output/new-data2.parquet") \
        .save()
    
    print("================Done================")