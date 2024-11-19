from flask import Flask, request, jsonify
import requests
import logging 

app = Flask(__name__)

SUBSCRIBER_URL = "http://localhost:7001/register"  # Change if using the second subscriber
CLIENT_URL = "http://localhost:8001"  # URL the subscriber will POST to
PORT = 8001  # Change for additional clients

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR) 

@app.route("/", methods=["POST"])
def receive_emoji():
    """
    Endpoint to receive emoji data from the subscriber.
    """
    data = request.json
    if not data:
        return jsonify({"error": "Invalid data"}), 400
    
    emoji = data.get("emoji")
    count = data.get("count")
    if emoji:
        print(emoji * count)  # Print emoji `count` times for better visibility
    else:
        print("Received data without an emoji.")
    return jsonify({"status": "received"}), 200

def register_with_subscriber():
    """
    Registers the client with the subscriber to start receiving POST data.
    """
    try:
        payload = {"client_url": CLIENT_URL}
        response = requests.post(SUBSCRIBER_URL, json=payload)
        if response.status_code in [200, 201]:
            print("Registered successfully with subscriber:", response.json())
        else:
            print(f"Failed to register. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Error while registering with subscriber: {e}")
        exit(1)

if __name__ == "__main__":
    # Register the client with the subscriber
    register_with_subscriber()

    # Start the Flask server
    print("Listening for emojis...")
    app.run(host="0.0.0.0", port=PORT)
