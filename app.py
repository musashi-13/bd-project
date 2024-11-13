import threading
import time
import json
from kafka import KafkaProducer
from flask import Flask, request, jsonify

# Initialize Flask app
app = Flask(__name__)

# Initialize Kafka producer with a 500 ms flush interval
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka topic name
topic_name = 'emoji_data_topic'

# Buffer for incoming emoji data
buffer = []

# Function to flush buffer data to Kafka
def flush_to_kafka():
    while True:
        time.sleep(0.5)  # 500 ms interval
        if buffer:
            for data in buffer:
                producer.send(topic_name, value=data)
            producer.flush()
            buffer.clear()  # Clear buffer after flush

# Start flush thread
flush_thread = threading.Thread(target=flush_to_kafka)
flush_thread.daemon = True
flush_thread.start()

@app.route('/submit_emoji', methods=['POST'])
def submit_emoji():
    data = request.get_json()
    if all(key in data for key in ('user_id', 'emoji_type', 'timestamp')):
        buffer.append(data)
        return jsonify({'status': 'Data buffered for Kafka'}), 200
    else:
        return jsonify({'error': 'Invalid data format'}), 400

if __name__ == '__main__':
    app.run(port=5000)

