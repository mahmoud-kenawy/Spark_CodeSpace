from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime as datetime 
# Initialize Spark
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TaxiDataAnalysis") \
    .getOrCreate()

# Set path to your downloaded file
input_path = "/workspaces/Spark_CodeSpace/data/raw/yellow_taxi/yellow_tripdata_2025-11.parquet"
df = spark.read.parquet(input_path)
# Q1 partioning the data into 4 partitions and writing it back to disk
'''
df.repartition(4).write.parquet("/workspaces/Spark_CodeSpace/data/processed/yellow_taxi/")
'''
#Q2 
'''
df_15_nov = df.where(col("tpep_pickup_datetime").startswith("2025-11-15"))
'''
#Q3



spark.stop()