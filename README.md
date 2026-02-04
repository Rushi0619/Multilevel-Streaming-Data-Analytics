# 📊 Multilevel-Streaming-Data-Analytics

**Using Apache Kafka, Apache Spark & Spark ML**

---

## 📌 Project Overview

This project implements an **end-to-end real-time Big Data streaming analytics pipeline** for financial news sentiment analysis.

The system ingests live news events using **Apache Kafka**, processes and enriches them using **Apache Spark Structured Streaming**, applies **distributed machine learning (Spark ML)** for sentiment prediction, and stores analytics results in **AWS S3**, enabling SQL-based querying through **Amazon Athena**.

A **multi-level analytics architecture** is used, where data is incrementally validated, structured, enriched, and analyzed at each stage.

---

## 🏗️ System Architecture

```
Financial News CSV
        ↓
Kafka Producer
        ↓
Kafka Topic
        ↓
Level-1 Consumer (Validation & Filtering)
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
Amazon Athena (SQL Analytics)
```

---

## 🧰 Technology Stack

| Layer             | Technology                        |
| ----------------- | --------------------------------- |
| Messaging         | Apache Kafka                      |
| Stream Processing | Apache Spark Structured Streaming |
| Machine Learning  | Spark ML (Logistic Regression)    |
| Storage           | Parquet, AWS S3                   |
| Query Engine      | Amazon Athena                     |
| Language          | Python                            |
| Cloud Platform    | AWS EC2                           |

---

## 📁 Project Structure

```
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
├── spark_ml_train.py
│
├── level2_output/
├── level3_ml_predictions_fixed/
│
├── level3_storage_consumer.py
├── level3_enriched_events.json
│
├── README.md
└── venv/
```

---

## ⚠️ Important Environment Note (Very Important)

### ❌ Why `venv` does NOT work for Spark ML

* Spark ML depends on **NumPy**
* Spark 3.5.x internally expects **`distutils`**
* Python **3.12+ removes `distutils`**
* Resulting errors:

  ```
  ModuleNotFoundError: No module named 'numpy'
  ModuleNotFoundError: No module named 'distutils'
  ```

### ✅ Solution Used

A **separate Conda environment** is used **only for Spark ML** execution.

| Component               | Environment       |
| ----------------------- | ----------------- |
| Kafka + Spark Streaming | Python `venv`     |
| Spark ML                | Conda (`sparkml`) |

This separation is **intentional and correct**.

---

## ⚙️ Prerequisites

### Infrastructure

* AWS EC2 (Ubuntu 22.04 / 24.04)
* Minimum: **4 vCPU, 8–16 GB RAM, 30+ GB storage**

### Software

* Docker & Docker Compose
* Java 11
* Apache Spark 3.5.x
* Python 3.x
* Conda (Miniconda / Anaconda)
* AWS CLI

---

## 🚀 Step-by-Step Execution Guide

### 🔹 Step 1: Start Kafka

```bash
docker-compose up -d
docker ps
```

---

### 🔹 Step 2: Python Environment (Kafka & Streaming)

```bash
python3 -m venv venv
source venv/bin/activate
pip install kafka-python pandas pyspark
```

---

### 🔹 Step 3: Start Kafka Producer

```bash
python producer.py
```

Streams financial news CSV data into Kafka.

---

### 🔹 Step 4: Level-2 Spark Streaming

```bash
spark-submit level2_spark_analytics.py
```

✔ Reads Kafka topic
✔ Parses JSON events
✔ Writes structured Parquet output

**Output:** `level2_output/`

---

### 🔹 Step 5: Verify Level-2 Output

```bash
spark-submit check_level2.py
```

---

### 🔹 Step 6: Create Conda Environment for Spark ML (Critical)

```bash
conda create -n sparkml python=3.11 -y
conda activate sparkml
pip install numpy
```

⚠️ All Spark ML steps **must** run in this environment.

---

### 🔹 Step 7: Spark ML Feature Engineering

```bash
spark-submit spark_ml_features.py
```

Generates:

* Tokenized text
* Stop-word removal
* TF-IDF feature vectors

---

### 🔹 Step 8: Spark ML Training & Corrections

```bash
spark-submit spark_ml_train.py
```

Inside this step:

* Loads Level-2 Parquet data
* Applies **rule-based financial sentiment corrections**
* Encodes labels
* Trains **Logistic Regression**
* Generates predictions & probabilities

**Output:** `level3_ml_predictions_fixed/`

---

### 🔹 Step 9: Upload Output to AWS S3

```bash
aws s3 cp level3_ml_predictions_fixed/ \
s3://<your-bucket>/level3_ml_predictions/ --recursive
```

---

### 🔹 Step 10: Query Using Amazon Athena

```sql
CREATE DATABASE financial_news_db;

CREATE EXTERNAL TABLE financial_news_db.sentiment_predictions (
  headline STRING,
  sentiment STRING,
  prediction DOUBLE,
  probability ARRAY<DOUBLE>
)
STORED AS PARQUET
LOCATION 's3://<your-bucket>/level3_ml_predictions/';
```

```sql
SELECT * 
FROM financial_news_db.sentiment_predictions 
LIMIT 10;
```

---

## 🧠 Machine Learning Summary

| Aspect      | Details                                 |
| ----------- | --------------------------------------- |
| Algorithm   | Logistic Regression (Spark ML)          |
| Features    | TF-IDF                                  |
| Classes     | Positive, Negative, Neutral             |
| Enhancement | Rule-based sentiment correction         |
| Reason      | Scalable, explainable, production-ready |

---

## 📈 Key Learnings

* Kafka & Spark Structured Streaming integration
* Multi-level analytics architecture
* Distributed machine learning with Spark ML
* Cloud-based analytics using S3 & Athena
* Environment isolation for Spark ML stability

---

## 🔮 Future Enhancements

* Replace Logistic Regression with LSTM (offline training)
* Real-time Spark ML inference from Kafka
* Amazon QuickSight dashboards
* Model versioning and evaluation metrics

---

## 👤 Author

**Rushikesh Ashok Ghotkar**
B.E. – Artificial Intelligence & Data Science
