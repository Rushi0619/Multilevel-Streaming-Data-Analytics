import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "test-topic"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

print("Kafka Consumer started. Waiting for messages...")

for message in consumer:
    raw_value = message.value.decode("utf-8", errors="ignore")

    try:
        data = json.loads(raw_value)
        print("Received JSON message:", data)
    except json.JSONDecodeError:
        print("Skipped non-JSON message:", raw_value)
