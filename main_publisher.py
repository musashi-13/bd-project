# main_publisher.py
import time
from kafka import KafkaConsumer, KafkaProducer
import json

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
input_topic = 'processed_emoji_data'  # Topic where consumer.py writes processed data
cluster_topics = ['cluster1_topic', 'cluster2_topic', 'cluster3_topic']  # Topics for each cluster

# Initialize Kafka consumer to read processed emoji data
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer to publish to clusters
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print("Main Publisher is running and publishing to clusters...")

# Loop to read from input_topic and publish to each cluster topic
for message in consumer:
    data = message.value
    print(f"Main Publisher received data: {data}")

    # Publish to each cluster topic
    for topic in cluster_topics:
        producer.send(topic, value=data)
        print(f"Published to {topic}: {data}")

    producer.flush()
    time.sleep(0.1)  # Add a slight delay if needed for demonstration purposes

