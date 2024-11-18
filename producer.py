import threading
import time
import json
from kafka import KafkaProducer
from queue import Queue
import logging

# Set up logging specifically for the producer
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka topic name
topic_name = 'emoji_data_topic'

# Shared queue to hold emoji data received from Flask app
data_queue = Queue()

# Function to flush buffered data to Kafka
def flush_to_kafka():
    while True:
        time.sleep(0.5)  # 500 ms interval
        # Flush only if there are items in the queue
        if not data_queue.empty():
            while not data_queue.empty():
                data = data_queue.get()
                producer.send(topic_name, value=data)
                logging.info(f"Flushed data to Kafka: {data}")
                data_queue.task_done()
            producer.flush()
            logging.info("Buffer flushed to Kafka.")

# Start the flushing thread
flush_thread = threading.Thread(target=flush_to_kafka)
flush_thread.daemon = True
flush_thread.start()

# Function to add data to the queue (called from Flask app)
def buffer_data_for_kafka(data):
    data_queue.put(data)

# Keep the main program running
if __name__ == "__main__":
    logging.info("Kafka producer is running and waiting for data...")
    try:
        while True:
            time.sleep(1)  # Keeps the program alive
    except KeyboardInterrupt:
        logging.info("Shutting down Kafka producer.")

