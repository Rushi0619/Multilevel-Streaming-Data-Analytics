import time
import json
import pandas as pd
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = "localhost:9092"  #where Kafka is running
TOPIC_NAME = "test-topic"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,           #how to connect to Kafka
    value_serializer=lambda v: json.dumps(v).encode("utf-8") #Python dictionary → JSON string → bytes
)

#This file:
#	Converts static CSV data into real-time stream
#	Acts as the data ingestion layer
#	Makes your project a true streaming system, not batch

print("Kafka CSV Producer started...")

# Load real CSV dataset
df = pd.read_csv("financial_news_events.csv")

print(f"Total records to stream: {len(df)}")

# Stream each row as a Kafka message
for index, row in df.iterrows():           #Iterate the rows of the DataFrame
    message = row.to_dict()

    producer.send(TOPIC_NAME, message)        #Sends the message to Kafka topic	(Asynchronous)
    print(f"Sent record {index + 1}: {message}")      #Logs each sent message

    time.sleep(1)  # simulate real-time feed (1 second per event)

producer.flush()         #Forces Kafka to send all buffered messages
print("Finished streaming all CSV records.")

