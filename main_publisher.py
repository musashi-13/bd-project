from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Kafka settings
KAFKA_BROKER = "localhost:9092"
MAIN_TOPIC = "main-pub-topic"

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/publish-aggregated-data', methods=['POST'])
def publish_aggregated_data():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Invalid input, data is required"}), 400
        
        # Publish aggregated data to Kafka
        producer.send(MAIN_TOPIC, value=data)
        producer.flush()
        return jsonify({"status": "success", "message": "Data sent to cluster publishers"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000, debug=True)
