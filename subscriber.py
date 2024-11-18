from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json
import threading

# Flask app for WebSocket
app = Flask(_name_)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
SUBSCRIBER_TOPIC = 'Cluster1_subscribers'

# Keep track of connected clients
connected_clients = []

# Kafka Consumer Thread
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        SUBSCRIBER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("Kafka Consumer listening for messages...")
    for message in consumer:
        data = message.value
        
        # Forward the message to all connected WebSocket clients
        for client in connected_clients:
            socketio.emit('emoji_update', data, to=client)

# Handle WebSocket connections
@socketio.on('connect')
def handle_connect():
    client_id = request.sid
    connected_clients.append(client_id)
    print(f"Client connected: {client_id}")
    emit('connection_ack', {"message": "Connected to subscriber"})

# Handle WebSocket disconnections
@socketio.on('disconnect')
def handle_disconnect():
    client_id = request.sid
    if client_id in connected_clients:
        connected_clients.remove(client_id)
        print(f"Client disconnected: {client_id}")

# Start the Kafka consumer thread
kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
kafka_thread.start()

if _name_ == "_main_":
    socketio.run(app, port=6000)
