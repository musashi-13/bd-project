# RR-Team-5-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

## Things to work on
* clients.py
* subscriber cleanup
* consumer aggregation

## How to run:

* `./start.sh`

_starts kafka and zookeeper. change the directory variable_
* `./topic.sh`

_creates or replace all the needed topics_

* `python3 app.py`

_listens to /send-emoji_
* `spark-submit spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumer.py`

_processes data. resets every batch. batch = 2 seconds. currently does not aggregate_
* `python3 main_pub.py`

_sends data to cluster publisher_
* `python3 cluster_pub.py`

_sends data to subscriber_
* `python3 subscriber.py`

_need to work on printing logic and interaction with frontend or smtg_