import json
from datetime import datetime
from kafka import KafkaConsumer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "test-topic"

# Output file (S3 / Athena friendly: JSON Lines)
OUTPUT_FILE = "level3_enriched_events.json"

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Level-3 Analytics Consumer started...")
print("Writing enriched events to:", OUTPUT_FILE)


# --------------------------------------------------
# PLACEHOLDER FOR LSTM SENTIMENT PREDICTION
# (Inference only – replace with real LSTM later)
# --------------------------------------------------
def lstm_predict_sentiment(headline: str) -> str:
    headline = headline.lower().strip()

    if "crash" in headline or "fall" in headline or "decline" in headline:
        return "Negative"
    elif "growth" in headline or "rise" in headline or "profit" in headline:
        return "Positive"
    else:
        return "Neutral"


# Open output file in append mode (stream-safe)
with open(OUTPUT_FILE, "a", encoding="utf-8") as file:
    for message in consumer:
        raw_value = message.value.decode("utf-8", errors="ignore")

        # Parse JSON safely
        try:
            data = json.loads(raw_value)
        except json.JSONDecodeError:
            continue

        # Validate headline
        headline = data.get("Headline")
        if not isinstance(headline, str):
            continue

        # -------- Level-3 Analytics (ML Inference) --------
        predicted_sentiment = lstm_predict_sentiment(headline)

        enriched_event = {
            "timestamp": datetime.utcnow().isoformat(),
            "Date": data.get("Date"),
            "Headline": headline,
            "Market_Event": data.get("Market_Event"),
            "Impact_Level": data.get("Impact_Level"),
            "Original_Sentiment": data.get("Sentiment"),
            "Predicted_Sentiment": predicted_sentiment,
            "Related_Company": data.get("Related_Company")
        }

        # Write JSON line (Athena / S3 compatible)
        file.write(json.dumps(enriched_event) + "\n")
        file.flush()

        print("LEVEL-3 STORED EVENT:", enriched_event)
