import threading
import requests
import json
import time
from datetime import datetime
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# API endpoint URL (ensure this matches the Gunicorn host/port)
url = 'http://10.0.2.15:5000/submit_emoji'

# Emoji data pool for simulation
emojis = ["😊", "😂", "😢", "❤️", "👍", "🎉", "🔥", "😎", "🤔", "👏"]

# Setup retry strategy for requests
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
session.mount('http://', HTTPAdapter(max_retries=retries))

# Function to simulate a client sending data
def send_emoji_data(client_id):
    for _ in range(200):  # Each client sends 50 messages
        data = {
            "user_id": str(client_id),
            "emoji_type": random.choice(emojis),
            "timestamp": datetime.utcnow().isoformat()
        }
        try:
            response = session.post(url, json=data)
            if response.status_code == 200:
                print(f"Client {client_id} sent: {data}")
            else:
                print(f"Client {client_id} failed with status {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Client {client_id} encountered an error: {e}")
        time.sleep(0.01)  # Slight delay to control request frequency

# Number of clients to simulate
num_clients = 20  # Adjust to control total concurrent clients

# List to hold thread objects
threads = []

# Start each client in its own thread
for i in range(num_clients):
    thread = threading.Thread(target=send_emoji_data, args=(i,))
    thread.start()
    threads.append(thread)

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All clients finished sending data.")

