from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Tokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
    StringIndexer
)
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import col, lower, when

# --------------------------------------------------
# 1. Initialize Spark
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("Spark-ML-Sentiment-Training-Final") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Load Level-2 data
# --------------------------------------------------
df = spark.read.parquet("level2_output")

# --------------------------------------------------
# 3. RULE-BASED SENTIMENT CORRECTION (PRE-TRAINING)
# --------------------------------------------------
df_corrected = df.withColumn(
    "Sentiment",
    when(
        lower(col("Headline")).rlike("tumbling|collapse|breach|turmoil|crisis|decline"),
        "Negative"
    ).when(
        lower(col("Headline")).rlike("surge|surges|oversubscribed|gains|rally|boom|jump"),
        "Positive"
    ).when(
        lower(col("Headline")).rlike("stable|maintains|unchanged|status quo"),
        "Neutral"
    ).otherwise(col("Sentiment"))
)

# --------------------------------------------------
# 4. STRICT DATA CLEANING (NO GHOST CLASSES)
# --------------------------------------------------
df_cleaned = df_corrected.select("Headline", "Sentiment") \
    .where(
        col("Headline").isNotNull() &
        col("Sentiment").isin("Positive", "Negative", "Neutral")
    )

# --------------------------------------------------
# 5. FEATURE ENGINEERING PIPELINE
# --------------------------------------------------
tokenizer = Tokenizer(
    inputCol="Headline",
    outputCol="words"
)

remover = StopWordsRemover(
    inputCol="words",
    outputCol="filtered"
)

hashingTF = HashingTF(
    inputCol="filtered",
    outputCol="rawFeatures",
    numFeatures=1000
)

idf = IDF(
    inputCol="rawFeatures",
    outputCol="features"
)

# --------------------------------------------------
# 6. LABEL ENCODING (FINAL — ONLY 3 CLASSES)
# --------------------------------------------------
label_indexer = StringIndexer(
    inputCol="Sentiment",
    outputCol="label",
    handleInvalid="skip"
)

# --------------------------------------------------
# 7. ML MODEL
# --------------------------------------------------
lr = LogisticRegression(
    maxIter=20,
    regParam=0.01,
    labelCol="label",
    featuresCol="features"
)

# --------------------------------------------------
# 8. PIPELINE
# --------------------------------------------------
pipeline = Pipeline(stages=[
    tokenizer,
    remover,
    hashingTF,
    idf,
    label_indexer,
    lr
])

# --------------------------------------------------
# 9. TRAIN MODEL
# --------------------------------------------------
model = pipeline.fit(df_cleaned)

# --------------------------------------------------
# 10. PREDICTION
# --------------------------------------------------
predictions = model.transform(df_cleaned)

final_output = predictions.select(
    "Headline",
    "Sentiment",
    "prediction",
    "probability"
)

final_output.show(10, truncate=False)

# --------------------------------------------------
# 11. STORE LEVEL-3 OUTPUT
# --------------------------------------------------
final_output.write \
    .mode("overwrite") \
    .parquet("level3_ml_predictions_fixed")

spark.stop()
