from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("SparkML-Feature-Engineering") \
    .getOrCreate()

# Load Level-2 data
df = spark.read.parquet("level2_output")

# Select required columns and drop nulls
df = df.select("Headline", "Sentiment").dropna()

# Tokenization
tokenizer = Tokenizer(
    inputCol="Headline",
    outputCol="words"
)

# Remove stop words
stopwords = StopWordsRemover(
    inputCol="words",
    outputCol="filtered_words"
)

# Convert text to TF features
hashingTF = HashingTF(
    inputCol="filtered_words",
    outputCol="raw_features",
    numFeatures=1000
)

# Apply IDF
idf = IDF(
    inputCol="raw_features",
    outputCol="features"
)

pipeline = Pipeline(stages=[
    tokenizer,
    stopwords,
    hashingTF,
    idf
])

# Fit and transform
model = pipeline.fit(df)
features_df = model.transform(df)

# Show results
features_df.select("Headline", "Sentiment", "features").show(5, truncate=False)

spark.stop()
