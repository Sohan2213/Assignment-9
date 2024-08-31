import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CustomerDataManagement").getOrCreate()
df = spark.read.format("csv").option("header", "true").load("/path/to/customer_dataset.csv")

from pyspark.sql.functions import col

duplicates_df = df.groupBy("Name", "Address").count().filter(col("count") > 1)

from pyspark.sql.functions import monotonically_increasing_id

master_df = df.join(duplicates_df, ["Name", "Address"], "left").withColumn("Master_ID", monotonically_increasing_id())

master_df.write.format("jdbc").option("url", "jdbc:sqlserver://server.database.windows.n
