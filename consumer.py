from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, ceil
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from kafka import KafkaProducer
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define schema for incoming emoji data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
input_topic = "emoji_data_topic"
output_topic = "processed_emoji_data"

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
    .option("startingOffsets", "latest") \
    .load()

# Transform data: Deserialize JSON
emoji_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Aggregation: Count emojis by type in 2-second windows
agg_df = emoji_df \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(window("timestamp", "2 seconds"), "emoji_type") \
    .agg(count("emoji_type").alias("emoji_count"))

# Compute compressed count
compressed_df = agg_df.withColumn(
    "compressed_count",
    ceil(col("emoji_count") / 1000)
).filter(col("compressed_count") > 0)

# Function to send aggregated results to Kafka
def send_to_kafka(aggregated_data):
    unique_rows = aggregated_data.distinct().collect()  # Ensure distinct rows
    for row in unique_rows:
        data = {
            "emoji_type": row['emoji_type'],
            "compressed_count": row['compressed_count']
        }
        try:
            processed_data_producer.send(output_topic, value=data)
            print(f"Sent to Kafka: {data}")
        except Exception as e:
            print(f"Failed to send data: {data}, error: {e}")

# Write aggregated data back to Kafka
agg_query = (
    compressed_df.writeStream
    .outputMode("update")
    .foreachBatch(lambda batch_df, batch_id: send_to_kafka(batch_df))
    .trigger(processingTime="2 seconds")
    .start()
)


# Await termination
agg_query.awaitTermination()



