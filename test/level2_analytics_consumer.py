import json
from collections import defaultdict
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "test-topic"

# Kafka consumer (raw bytes)
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Level-2 Analytics Consumer started...")

"""
LEVEL-2 ANALYTICS LOGIC
----------------------
Aggregations in real time:
1. Sentiment count
2. Impact level count
3. Company-wise event count

This represents decision-support analytics.
"""

# Aggregation stores
sentiment_count = defaultdict(int)
impact_count = defaultdict(int)
company_count = defaultdict(int)

for message in consumer:
    raw_value = message.value.decode("utf-8", errors="ignore")

    try:
        data = json.loads(raw_value)
    except json.JSONDecodeError:
        continue  # ignore non-JSON messages

    sentiment = data.get("Sentiment")
    impact = data.get("Impact_Level")
    company = data.get("Related_Company")

    if not sentiment or not impact or not company:
        continue  # ignore incomplete records

    # Update aggregations
    sentiment_count[sentiment] += 1
    impact_count[impact] += 1
    company_count[company] += 1

    # Print live analytics snapshot
    print("\n📊 LEVEL-2 ANALYTICS SNAPSHOT")
    print("Sentiment Distribution:", dict(sentiment_count))
    print("Impact Distribution:", dict(impact_count))
    print("Top Companies:", dict(company_count))
