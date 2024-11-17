from kafka import KafkaConsumer
import json
import time

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SUBSCRIBER_TOPIC = 'Cluster1_subscribers'

consumer = KafkaConsumer(
    SUBSCRIBER_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

emoji_counts = {"😂": 0, "😭": 0, "🥳": 0, "😍": 0, "😡": 0}

print("Subscriber listening to cluster data...")

for message in consumer:
    data = message.value
    emoji = data['emoji_type']
    
    if emoji in emoji_counts:
        emoji_counts[emoji] += data['compressed_count']

    # Display the emoji stream
    compressed_stream = "".join([emoji * emoji_counts[emoji] for emoji in emoji_counts])
    print(f"Current Emoji Stream: {compressed_stream}", end="\r")
    time.sleep(0.1)

