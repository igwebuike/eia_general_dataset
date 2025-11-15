from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.csv("s3://your-bucket/raw/general_dataset.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.fill(0)

df.write.mode("overwrite").parquet("s3://your-bucket/processed/general_dataset")
print("Glue ETL complete for General Dataset")
