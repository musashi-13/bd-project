# RR-Team-5-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

## Team Members
* PES1UG22CS189 - Dinesh Kumar L C - [dineshhdini](https://github.com/dineshhdini)
* PES1UG22CS253 - Jayanth Ramesh - [Matr1XxxX](https://github.com/Matr1XxxX)
* PES1UG22CS275 - Karan Prasad Hathwar - [musashi-13](https://github.com/muasashi-13)
* PES1UG22CS279 - Kaushik Bhat - [kaushik-bhat](https://github.com/kaushik-bhat)

## Problem statement

> The project's goal is to design and implement a scalable system that enhances the viewer experience on platforms like Hotstar by capturing and processing billions of user-generated emojis in real time during live sporting events. This will involve creating a horizontally scalable architecture using frameworks such as Kafka for data streaming and Spark for real-time data processing. The system will enable high concurrency and low latency, ensuring user interactions are seamless.

## How to run:

* `./start.sh`
  * _starts kafka and zookeeper. change the directory variable_
* `./topic.sh`
  * _creates or replace all the needed topics_
* `python3 app.py`
  * _listens to /send-emoji_
* `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 main_publisher.py`
  * _processes data. resets every batch. batch = 2 seconds. aggregates every 1000 emojis. sends emojis to main-pub-topic._
* `python3 cluster_publisher.py CLUSTER-TOPIC=cluster1-emoji-topic `
  * _reads from main-pub-topic. there are 3 clusters each with its own topic as clusterx-pub-topic. each cluster has 2 subscribers that read from the respective topic_
* `CLUSTER_TOPIC=clusterx-emoji-topic python3 subscriber1.py` and `CLUSTER_TOPIC=clusterx-emoji-topic python3 subscriber2.py`
  * _reads from the cluster topic specified in command (x can be 1, 2, or 3) and gets ports assigned based on it. each subscriber has 3 functions, notify-client to send emoji, register to register the client and deregister to do the opposite._
* `PORT=8001 python3 client.py`
  * _sends emojis to app.py that is listening to /send-emoji and receives emojis from the subscriber it is registered to. registration happens via polling since each subscriber can have only 2 clients. prints recieved emojis as a stream_

## Emoji Processing System - Project Checklist

#### **API Endpoint Setup**
- [x] Implemented a Flask (or Express.js) HTTP endpoint to receive POST requests with emoji data.
- [x] Data sent in the request includes: User ID, Emoji type, and Timestamp.
- [x] Asynchronously writes the data to the Kafka producer.
  
#### **Kafka Producer**
- [x] Set up Kafka producer to asynchronously send emoji data to the Kafka broker.
- [x] Configured the producer to flush messages every 500 milliseconds.

#### **Spark Streaming Job**
- [x] Created a Spark Streaming job to consume data from Kafka.
- [x] Processed emoji data in micro-batches with a 2-second interval.
- [x] Aggregated emoji data, reducing 1000+ emojis of the same type to 1 emoji.

#### **Pub-Sub Architecture**
- [x] Set up the main publisher to send processed emoji data to cluster publishers.
- [x] Established at least 3 clusters with cluster publishers and 3 or more subscribers per cluster.
- [x] Ensured real-time delivery of processed data to all subscribers.
- [x] Implemented client registration API to allow clients to join or leave without disrupting the workflow.
  
#### **Client Interaction**
- [x] Allowed clients to register and receive real-time emoji updates.
- [x] Clients who send emoji data also receive the aggregated results in real-time.

#### **Demo**
- [x] Simulated multiple clients sending emoji data concurrently.
- [x] Demonstrated Kafka producer writing data asynchronously and Spark engine processing it.
- [x] Showed a smooth registration and deregistration process for clients during live streaming.