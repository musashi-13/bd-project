from flask import Flask, request, jsonify
from producer import buffer_data_for_kafka  # Import the buffer function from producer.py

# Initialize Flask app
app = Flask(__name__)

@app.route('/submit_emoji', methods=['POST'])
def submit_emoji():
    data = request.get_json()
    if all(key in data for key in ('user_id', 'emoji_type', 'timestamp')):
        buffer_data_for_kafka(data)  # Add data to the queue for Kafka
        return jsonify({'status': 'Data buffered for Kafka'}), 200
    else:
        return jsonify({'error': 'Invalid data format'}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

