# SAP Project     (Glue Job 1)      
#  Read data from "Raw Zone(Bucket)" to "Transform Zone" (2 files are read)
#  Do Source to Target Mapping of table (column mapping) according to  STTM Documentation
#  Save the file to "Transform Zone" in "parquet" format  


import pyspark
from pyspark.sql import *

from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace
import re
from datetime import datetime

if __name__ == "__main__":
    
    currentdate = datetime.now().strftime("%Y-%m-%d")
    print("currentdate: ", currentdate)


    print("========== Glue Job 1 ================")

    spark = SparkSession \
        .builder \
        .appName("SAP_proj_job1") \
        .master("local[2]") \
        .getOrCreate()
    
#1. read csv file 1
    
    df_csv = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3://sap-spark-raw-zone/input-files/new_data1.csv")
     
     
# .load("s3://sap-spark-raw-zone/2_sales_records.csv")
    
    print(df_csv.columns)
    print("=================csv file1=======================")
    
#1. read csv file 2

    df_csv2 = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("s3://sap-spark-raw-zone/input-files/new_data2.csv")
    
    
# .load("s3://sap-spark-raw-zone/new_sales_data.csv")
        
    print( df_csv2.columns)
    print("=================csv file2===================")
    

# #2. read json file
#     df_json = spark.read.json("s3://sap-spark-raw-zone/sample_sales_SAP_proj.json")        
#     print(df_json.columns)
#     print(type(df_json.columns))
    

#3. Compare csv file header(schema) with sample json file header(schema)
    # l1 = df_csv.columns
    # l3 = df_json.columns

    # if l1 == l3: 
    #     print ("\n 1.The lists l1 and l3 are the same")                         
    # else: 
    #     print ("\n 1.The lists l1 and l3 are not the same") 
        
    # print("========================================")
        
        
# 4. check if read csv file size is non-zero

    rows = df_csv.count()
    cols = len(df_csv.columns)
    if rows != 0 and cols != 0:
        print("\n 2. File1 received is of non-zero size ")
    else:
        print("\n 2. File1 received is empty")
        
    print("==================csv1======================")
    
    rows2 = df_csv2.count()
    cols2 = len(df_csv2.columns)
    if rows2 != 0 and cols2 != 0:
        print("\n 2. File2 received is of non-zero size ")
    else:
        print("\n 2. File2 received is empty")
        
    print("==================csv2======================")
        
# 5. convert header to lowercase
    df_csv_lower = df_csv.select([F.col(x).alias(x.lower()) for x in df_csv.columns])
    print("\n 3. headers in lowercase")
    print(df_csv_lower.columns)
    
    print("=================csv1=======================")
    
    df_csv_lower2 = df_csv2.select([F.col(x).alias(x.lower()) for x in df_csv2.columns])
    print("\n 3. headers in lowercase")
    print(df_csv_lower2.columns)
    
    print("==================csv2======================")
    
    
# 6. Remove special characters from the header(column names)

#      for c in df_csv.columns:                                                   # method 1
#         df_csv = df_csv.withColumnRenamed(c, c.replace(" ", "_"))               # replace spaces in column names by "_"   

    df_csv_special = df_csv_lower.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","_",col)) for col in df_csv_lower.columns])
    
    print("\n 4. headers after removal of special characters")
    print(df_csv_special.columns)
    
    print("=================csv1=======================")
    
    df_csv_special2 = df_csv_lower2.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","_",col)) for col in df_csv_lower2.columns])
    
    print("\n 4. headers after removal of special characters")
    print(df_csv_special2.columns)
    
    print("=================csv2=======================")
    
# 7. Remove spaces from both sides from the header(column names)
#     df_csv = df_csv.trim()
    # for col in df_csv_special.columns:
    #     df_csv_trim = df_csv_special.withColumn(col, F.trim(col) )
    
    # df_csv_trim = df_csv_special.select([F.trim(s).alias(s) for s in df_csv_special.columns])

    # print("\n 5. headers after removal of spaces(trimmed header)")
    # print(df_csv_trim.columns)
    
    # print("=================================================================================")
# 8. Save spark-dataframe as parquet file to transform zone

    df_csv_special.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path","s3://sap-spark-transform-zone/output/new-data1.parquet") \
        .save()
        
    # .option("path","s3://sap-spark-transform-zone/output"+ "/" + currentdate + "/" + "new-data1.parquet") \
    
    print("==================csv1======================")
    df_csv_special2.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path","s3://sap-spark-transform-zone/output/new-data2.parquet") \
        .save()
        
    # .option("path","s3://sap-spark-transform-zone/output"+ "/" + currentdate + "/" + "new-data2.parquet") \                   to save the files according to current date
    print("==================csv2======================")
    print("Done...")