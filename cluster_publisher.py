from kafka import KafkaConsumer, KafkaProducer
import json
import sys

# Ensure the cluster topic is provided as a command-line argument
if len(sys.argv) < 2:
    print("Usage: python cluster_publisher.py <cluster_topic>")
    sys.exit(1)

# Get the cluster topic from the command-line arguments
cluster_topic = sys.argv[1]

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SUBSCRIBER_TOPIC = f"{cluster_topic}_subscribers"

# Initialize Kafka consumer to listen to the data published to the cluster topic
consumer = KafkaConsumer(
    cluster_topic,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Kafka producer to publish data to the subscriber topic
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Continuously listen for messages on the cluster topic and forward them
for message in consumer:
    data = message.value
    producer.send(SUBSCRIBER_TOPIC, value=data)
    print(f"Cluster Publisher received data: {data}")
    producer.flush()

