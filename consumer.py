from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, floor
from pyspark.sql.types import StructType, StringType
from kafka import KafkaProducer
import json

# Initialize Spark session
spark = SparkSession.builder.appName("EmojiConsumer").getOrCreate()

# Set log level to ERROR to suppress unwanted logs
spark.sparkContext.setLogLevel("ERROR")

# Define schema for incoming emoji data
schema = StructType().add("client_id", StringType()).add("emoji", StringType()).add("timestamp", StringType())

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
input_topic = "emoji-topic"
output_topic = "aggregated-emoji-topic"

# Initialize Kafka producer for sending aggregated data
processed_data_producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .load()

# Parse incoming JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Drop timestamp as it isn't required for aggregation
parsed_df = parsed_df.drop("timestamp")

# Aggregation: Count emojis by type
aggregated_df = parsed_df.groupBy(col("emoji")).agg(count("*").alias("emoji_count"))

# Compute compressed count: For every 10 emojis, increment the compressed count by 1
aggregated_df_compressed = aggregated_df.withColumn(
    "compressed_count", floor(col("emoji_count") / 10)
)

# Function to send aggregated results to Kafka
def send_to_kafka(aggregated_data):
    unique_rows = aggregated_data.distinct().collect()  # Ensure distinct rows
    for row in unique_rows:
        # Prepare the data to send to Kafka
        data = {
            "emoji_type": row['emoji'],
            "count": row['emoji_count'],
            "compressed_count": row['compressed_count']
        }
        
        # Send data to Kafka topic
        try:
            processed_data_producer.send(output_topic, value=data)
        except Exception as e:
            print(f"Failed to send data to Kafka topic: {data}, error: {e}")

# Write aggregated data to Kafka using foreachBatch
agg_kafka_query = (
    aggregated_df_compressed.writeStream
    .outputMode("update")
    .foreachBatch(lambda batch_df, batch_id: send_to_kafka(batch_df))  # Process each batch
    .trigger(processingTime="2 seconds")  # Trigger every 2 seconds
    .start()
)

# Write aggregated data to the console in table format
agg_console_query = (
    aggregated_df_compressed.writeStream
    .outputMode("complete")  # This mode will print the entire aggregated output
    .format("console")  # Print in a tabular format in the console
    .trigger(processingTime="20 seconds")  # Trigger every 2 seconds
    .start()
)

# Await termination for both streams
agg_kafka_query.awaitTermination()
agg_console_query.awaitTermination()

