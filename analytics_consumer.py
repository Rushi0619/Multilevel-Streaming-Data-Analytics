import json
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "test-topic"

# Create Kafka consumer (raw bytes, manual decoding)
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Level-1 Analytics Consumer started...")

"""
LEVEL-1 ANALYTICS LOGIC
----------------------
1. Discard non-JSON messages
2. Clean missing values
3. Filter only HIGH / MEDIUM impact events
4. Select important fields for next analytics level
"""

for message in consumer:   #Infinite loop
    raw_value = message.value.decode("utf-8", errors="ignore")

    try:
        data = json.loads(raw_value)
    except json.JSONDecodeError:
        # Ignore old / plain-text messages
        continue

    # ----- Basic Cleaning -----
    impact = data.get("Impact_Level")
    sentiment = data.get("Sentiment")
    headline = data.get("Headline")

    if not impact or not sentiment or not headline:
        continue  # drop incomplete records

    # ----- Filtering (Level-1 Analytics) -----
    if impact not in ["High", "Medium"]:
        continue  # ignore low-impact events

    # ----- Select Important Fields -----
    analytics_event = {
        "Date": data.get("Date"),
        "Headline": headline,
        "Market_Event": data.get("Market_Event"),
        "Sentiment": sentiment,
        "Impact_Level": impact,
        "Related_Company": data.get("Related_Company")
    }

    print("LEVEL-1 ANALYTICS EVENT:", analytics_event)
