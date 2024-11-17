import socketio
import sys

if len(sys.argv) != 2:
    print("Usage: python subscriber.py <cluster_id>")
    sys.exit(1)

cluster_id = sys.argv[1]

# Initialize Socket.IO client
sio = socketio.Client()

@sio.event
def connect():
    print(f"Connected to cluster {cluster_id}")

@sio.event
def reaction_update(data):
    print(f"Received update for cluster {cluster_id}: {data}")

@sio.event
def disconnect():
    print("Disconnected from server")

try:
    sio.connect('http://localhost:5001')
    sio.emit('join', {"cluster_id": cluster_id})
    sio.wait()
except Exception as e:
    print(f"Error: {e}")

