📊 Multi-Level Real-Time Financial News Analytics using Kafka, Spark & Spark ML

📌 Project Overview
This project implements an end-to-end Big Data streaming analytics pipeline for financial news.
It ingests real-time news events using Apache Kafka, processes them using Apache Spark Structured Streaming, applies distributed machine learning (Spark ML) for sentiment prediction, and stores analytics results in AWS S3, enabling SQL-based querying via Amazon Athena.
The system follows a multi-level analytics architecture, gradually enriching data at each stage.

🏗️ Architecture Flow
Financial News CSV
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Level-1 Consumer (Python – validation/filtering)
        ↓
Spark Structured Streaming (Level-2)
        ↓
Parquet Storage (Level-2 Output)
        ↓
Spark ML (TF-IDF + Logistic Regression)
        ↓
Rule-Based Financial Corrections
        ↓
Parquet Storage (Level-3 Output)
        ↓
AWS S3
        ↓
Amazon Athena (SQL Queries)


🧰 Technology Stack
Layer
Technology
Messaging
Apache Kafka
Stream Processing
Apache Spark Structured Streaming
Machine Learning
Spark ML (Logistic Regression)
Storage
Parquet, AWS S3
Query Engine
Amazon Athena
Language
Python
Cloud
AWS EC2


📁 Actual Project Structure (Verified)
kafka-project/
│
├── docker-compose.yml
├── financial_news_events.csv
│
├── producer.py
├── consumer.py
├── analytics_consumer.py
│
├── level2_spark_analytics.py
├── level2_analytics_consumer.py
├── check_level2.py
│
├── spark_ml_features.py
├── spark_ml_train.py          ← Spark ML + rule-based correction logic
│
├── level2_output/
├── level3_ml_predictions_fixed/
│
├── level3_storage_consumer.py
├── level3_enriched_events.json
│
├── README.md
└── venv/


⚠️ Very Important Environment Note (READ THIS)
❌ Why venv DOES NOT work for Spark ML
Spark ML internally depends on NumPy
Spark (3.5.x) still expects distutils
Python 3.12+ removed distutils
Result:
ModuleNotFoundError: No module named 'numpy'
ModuleNotFoundError: No module named 'distutils'


✅ Solution Used in This Project
We created a separate Conda environment (sparkml) specifically for Spark ML execution.
👉 Kafka + Spark Streaming → venv
👉 Spark ML → conda (sparkml)
This separation is intentional and correct.

⚙️ Prerequisites
EC2 Instance
Ubuntu 22.04 / 24.04
Minimum recommended:
4 vCPU
8–16 GB RAM
30+ GB storage
Software
Docker & Docker Compose
Java 11
Apache Spark 3.5.x
Python 3.x
Conda (Miniconda or Anaconda)
AWS CLI

🚀 Step-by-Step Execution

🔹 STEP 1: Start Kafka
docker-compose up -d
docker ps


🔹 STEP 2: Python Environment for Kafka & Streaming
python3 -m venv venv
source venv/bin/activate
pip install kafka-python pandas pyspark


🔹 STEP 3: Start Kafka Producer
python producer.py

Streams CSV data into Kafka topic.

🔹 STEP 4: Level-2 Spark Streaming (Structured Streaming)
spark-submit level2_spark_analytics.py

✔ Reads Kafka topic
✔ Parses JSON
✔ Stores structured Parquet data
Output:
level2_output/


🔹 STEP 5: Verify Level-2 Output
spark-submit check_level2.py


🔹 STEP 6: Create Conda Environment for Spark ML (CRITICAL)
conda create -n sparkml python=3.11 -y
conda activate sparkml
pip install numpy

👉 All Spark ML steps must be run inside this environment

🔹 STEP 7: Spark ML Feature Engineering
spark-submit spark_ml_features.py

Creates:
Tokenized text
Stop-word removal
TF-IDF feature vectors

🔹 STEP 8: Spark ML Training + Rule-Based Corrections
spark-submit spark_ml_train.py

What happens inside spark_ml_train.py:
Loads Level-2 Parquet
Applies rule-based financial corrections
(e.g., “breach”, “collapse” → Negative)
Encodes labels using StringIndexer
Trains Logistic Regression
Generates predictions & probabilities
Stores results as Parquet
Output:
level3_ml_predictions_fixed/


🔹 STEP 9: Upload Level-3 Output to S3
aws s3 cp level3_ml_predictions_fixed/ \
s3://<your-bucket-name>/level3_ml_predictions/ \
--recursive


🔹 STEP 10: Query via Amazon Athena
CREATE DATABASE financial_news_db;

CREATE EXTERNAL TABLE financial_news_db.sentiment_predictions (
  Headline STRING,
  Sentiment STRING,
  prediction DOUBLE,
  probability ARRAY<DOUBLE>
)
STORED AS PARQUET
LOCATION 's3://<your-bucket-name>/level3_ml_predictions/';

SELECT * FROM financial_news_db.sentiment_predictions LIMIT 10;


🧠 Machine Learning Summary
Algorithm: Logistic Regression (Spark ML)
Features: TF-IDF
Classes: Positive, Negative, Neutral
Enhancement: Rule-based sentiment correction
Reason: Scalable, explainable, production-friendly

📈 Key Learnings
Kafka + Spark Streaming integration
Multi-level analytics architecture
Distributed ML using Spark ML
Cloud storage & SQL analytics
Environment isolation for Spark ML stability

🔮 Future Scope
Replace Logistic Regression with LSTM (offline training)
Real-time Spark ML inference from Kafka
QuickSight dashboards
Model versioning & evaluation metrics

👤 Author
Rushikesh Ashok Ghotkar
B.E. – Artificial Intelligence & Data Science
