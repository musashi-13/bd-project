import requests
import random
import time
import sys

if len(sys.argv) != 3:
    print("Usage: python clients.py <client_id>")
    sys.exit(1)

client_id = sys.argv[1]
interval = float(sys.argv[2])

EMOJIS = ["😂", "😭", "🥳", "😍", "😡"]

# Send 200 emojis
for i in range(200):
    emoji = random.choice(EMOJIS)
    timestamp = time.time()
    payload = {
        "client_id": client_id,
        "emoji": emoji,
        "timestamp": timestamp
    }
    try:
        response = requests.post('http://localhost:5000/send_emoji', json=payload)
        print(f"Client {client_id} sent {emoji} - Status: {response.status_code}")
    except Exception as e:
        print(f"Error sending data: {e}")
    
    # Sleep for the specified interval (e.g., 2 seconds or less)
    time.sleep(0.005)

print(f"Client {client_id} has sent 200 emojis.")

