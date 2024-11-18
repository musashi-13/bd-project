from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
CLUSTER_TOPIC = "cluster-emoji-topic"  # Topic to which the cluster publisher pushes data

consumer = KafkaConsumer(
    CLUSTER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id="cluster-subscribers",  # Consumer group for subscribers
    auto_offset_reset="earliest"
)

def forward_to_clients():
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        
        # Simulate sending data to clients (for demo purposes, print it)
        print(f"Sending aggregated data to clients: {data}")
        # In a real-world scenario, you could use WebSockets or a similar technology to send data to clients

if __name__ == "__main__":
    forward_to_clients()