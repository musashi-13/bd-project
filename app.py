from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import threading
import time

app = Flask(__name__)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Global variable to hold messages before sending to Kafka
message_queue = []

def periodic_flush():
    """Flush the producer every 0.5 seconds."""
    while True:
        time.sleep(0.5)
        if message_queue:
            # Send all accumulated messages
            for message in message_queue:
                producer.send('emoji-topic', value=message)
            producer.flush()  # Flush all messages
            message_queue.clear()  # Clear the queue after sending
        else:
            producer.flush()  # Flush any lingering messages

# Start a background thread to periodically flush
thread = threading.Thread(target=periodic_flush, daemon=True)
thread.start()

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    try:
        data = request.get_json()
        # Add the message to the queue
        message_queue.append(data)
        return jsonify({"status": "success", "data": data}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(port=5000, debug=True)

