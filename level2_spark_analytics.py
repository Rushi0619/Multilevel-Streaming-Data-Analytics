from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Level2-News-Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("Date", StringType()) \
    .add("Headline", StringType()) \
    .add("Source", StringType()) \
    .add("Market_Event", StringType()) \
    .add("Sentiment", StringType()) \
    .add("Impact_Level", StringType()) \
    .add("Related_Company", StringType())

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

level2_df = parsed_df.select(
    "Date",
    "Headline",
    "Market_Event",
    "Sentiment",
    "Impact_Level",
    "Related_Company"
)

query = (
    level2_df
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", "level2_output")
    .option("checkpointLocation", "checkpoint_level2")
    .start()
)

query.awaitTermination()

