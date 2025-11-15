import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Bronze table for General Dataset")
def bronze():
    return (
        spark.read.format("csv").option("header", True)
        .load(f"dbfs:/mnt/raw/general_dataset")
    )

@dlt.table(comment="Silver table for General Dataset")
def silver():
    df = dlt.read("bronze")
    return df.dropDuplicates().na.fill(0)
