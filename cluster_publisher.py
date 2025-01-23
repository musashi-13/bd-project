# cluster_publisher.py
import time
from kafka import KafkaConsumer, KafkaProducer
import json
import sys

# Get cluster topic from command line arguments
if len(sys.argv) < 2:
    print("Usage: python cluster_publisher.py <cluster_topic>")
    sys.exit(1)

cluster_topic = sys.argv[1]  # Topic name for this cluster

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
subscriber_topic = f"{cluster_topic}_subscribers"  # Subscribers' topic

# Initialize Kafka consumer to read data for this cluster
consumer = KafkaConsumer(
    cluster_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer to publish to subscribers in the cluster
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(f"Cluster Publisher for {cluster_topic} is running...")

# Loop to read from cluster_topic and publish to subscriber topic
for message in consumer:
    data = message.value
    print(f"Cluster Publisher received data for {cluster_topic}: {data}")

    # Publish to subscriber topic
    producer.send(subscriber_topic, value=data)
    print(f"Published to {subscriber_topic}: {data}")

    producer.flush()
    time.sleep(0.1)  # Slight delay for demonstration purposes

