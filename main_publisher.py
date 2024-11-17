from kafka import KafkaConsumer, KafkaProducer
import json
import random

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
INPUT_TOPIC = 'aggregated-emoji-topic'

# Initialize Kafka consumer for aggregated data
consumer = KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer for sending data to cluster publishers
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Define available clusters
CLUSTERS = ["Cluster1", "Cluster2", "Cluster3", "Cluster4", "Cluster5"]

def assign_to_cluster(data):
    cluster = random.choice(CLUSTERS)  # Simple load balancing
    producer.send(cluster, value=data)  # Send data to the assigned cluster topic

print("Main Publisher is listening to aggregated emoji data...")

# Listen for incoming aggregated emoji data
for message in consumer:
    data = message.value
    print(f"Main Publisher received data: {data}")
    
    # Assign data to a cluster dynamically
    assign_to_cluster(data)

