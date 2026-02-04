from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check-Level2") \
    .getOrCreate()

df = spark.read.parquet("level2_output")

df.printSchema()
df.show(5, truncate=False)

spark.stop()
