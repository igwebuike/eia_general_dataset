from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("eia_general_dataset").getOrCreate()

df = spark.read.csv("input.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.fill(0)

df.write.mode("overwrite").parquet("output/general_dataset")
print("PySpark ETL complete for General Dataset")
