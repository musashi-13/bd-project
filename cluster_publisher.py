from kafka import KafkaConsumer, KafkaProducer
import json

KAFKA_BROKER = "localhost:9092"
MAIN_TOPIC = "main-pub-topic"  # Topic from main publisher
CLUSTER_TOPIC = "cluster-emoji-topic"  # Cluster's specific topic

consumer = KafkaConsumer(
    MAIN_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="cluster-consumers",  # Consumer group for cluster
    auto_offset_reset="earliest"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def distribute_data():
    for message in consumer:
        # Message from main publisher, forward to cluster subscribers
        data = json.loads(message.value.decode('utf-8'))
        producer.send(CLUSTER_TOPIC, value=data)
        producer.flush()
        print(f"Forwarded aggregated data to cluster: {data}")

if __name__ == "__main__":
    distribute_data()