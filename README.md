# RR-Team-5-emostream-concurrent-emoji-broadcast-over-event-driven-architecture

## Things to work on
* clients.py
* subscriber cleanup
* consumer aggregation

## How to run:

* ./start.sh 
`starts kafka and zookeeper. change the directory variable`
* ./topic.sh
`creates or replace all the needed topics`
* python3 app.py
`listens to /send-emoji`
* spark-submit spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 consumer.py
`processes data. resets every batch. batch = 2 seconds. currently does not aggregate`
* python3 main_pub.py
`sends data to cluster publisher`
* python3 cluster_pub.py
`sends data to subscriber`
* python3 subscriber.py
`need to work on printing logic and interaction with frontend or smtg`