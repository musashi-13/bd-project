import requests
import random
from datetime import datetime
import time
import asyncio
import websockets
import threading

# List of emojis to simulate
EMOJIS = ["😊", "😂", "❤️", "🔥", "🎉"]

# Endpoints for sending and receiving emojis
SEND_EMOJI_URL = "http://localhost:5000/send-emoji"  # API for sending emojis
SUBSCRIBER_API_URL = "http://localhost:7000/register"  # API to register with subscriber
CLIENT_WEBSOCKET_URL = "ws://localhost:7000/ws"  # Subscriber WebSocket endpoint

# Function to send emojis to the main app
def send_emoji():
    for _ in range(100):  # Simulate 100 requests
        payload = {
            "user_id": f"user{random.randint(1, 100)}",
            "emoji_type": random.choice(EMOJIS),
            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")  # UTC timestamp
        }
        try:
            response = requests.post(SEND_EMOJI_URL, json=payload)
            if response.status_code == 200:
                print(f"Emoji sent successfully: {payload}")
            else:
                print(f"Failed to send emoji: {response.json()}")
        except Exception as e:
            print(f"Error sending emoji: {e}")
        time.sleep(0.1)  # 100 ms delay between requests

    print("Finished sending emojis.")


# WebSocket handler for listening to subscriber messages
async def listen_to_subscriber():
    print(f"Listening to subscriber at {CLIENT_WEBSOCKET_URL}...")
    try:
        async with websockets.connect(CLIENT_WEBSOCKET_URL) as websocket:
            while True:
                try:
                    message = await websocket.recv()
                    print(f"Received emoji update from subscriber: {message}")
                except websockets.ConnectionClosed:
                    print("WebSocket connection closed. Reconnecting...")
                    break
    except Exception as e:
        print(f"Error connecting to WebSocket: {e}")


# Main function to manage sending and receiving
def main():
    # Start emoji sending in a separate thread
    sending_thread = threading.Thread(target=send_emoji)
    sending_thread.start()

    # Listen to subscriber in the main thread
    try:
        asyncio.run(listen_to_subscriber())
    except KeyboardInterrupt:
        print("Shutting down client...")
        sending_thread.join()  # Wait for sending thread to finish
        print("Client shut down gracefully.")


if __name__ == "__main__":
    main()
