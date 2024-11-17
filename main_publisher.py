import socketio

# Create a Socket.IO server
sio = socketio.Server(cors_allowed_origins="*")
app = socketio.WSGIApp(sio)

CLUSTER_MAP = {
    "😂": "Cluster1",
    "😭": "Cluster2",
    "🥳": "Cluster3",
    "😍": "Cluster4",
    "😡": "Cluster5"
}

@sio.event
def connect(sid, environ):
    print(f"Subscriber {sid} connected.")

@sio.event
def disconnect(sid):
    print(f"Subscriber {sid} disconnected.")

def distribute_to_clusters(data):
    for emoji, count in data.items():
        cluster = CLUSTER_MAP.get(emoji, "DefaultCluster")
        sio.emit("reaction_update", {"emoji": emoji, "count": count}, to=cluster)

if __name__ == "__main__":
    from eventlet import wsgi
    import eventlet

    print("Main publisher running...")
    wsgi.server(eventlet.listen(('localhost', 5001)), app)

