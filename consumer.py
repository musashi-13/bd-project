from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr
from pyspark.sql.types import StructType, StringType, TimestampType, StructField

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Schema for incoming emoji data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read from Kafka
kafka_bootstrap_servers = "localhost:9092"
input_topic = "emoji_data_topic"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Transform data
emoji_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Aggregation: count emojis by type in 2-second windows
agg_df = emoji_df \
    .groupBy(window("timestamp", "2 seconds"), "emoji_type") \
    .agg(count("emoji_type").alias("emoji_count")) \
    .select("emoji_type", (col("emoji_count") / 1000).cast("int").alias("compressed_count"))

# Output to console (or you can send it back to Kafka if needed)
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="2 seconds") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()

