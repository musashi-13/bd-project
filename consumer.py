from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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

# Kafka setup
kafka_bootstrap_servers = "localhost:9092"
input_topic = "emoji_data_topic"

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Transform data: Deserialize JSON
emoji_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Aggregation: Count emojis by type in 2-second windows and apply compression
agg_df = emoji_df \
    .groupBy(window("timestamp", "2 seconds"), "emoji_type") \
    .agg(count("emoji_type").alias("emoji_count")) \
    .select(
        "emoji_type",
        # Apply compression: Set count to 1 if between 1 and 1000, otherwise 0
        when((col("emoji_count") > 0) & (col("emoji_count") <= 1000), 1).otherwise(0).alias("compressed_count")
    )

# Output the aggregated and compressed data to the console
agg_query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .trigger(processingTime="2 seconds") \
    .option("truncate", "false") \
    .start()

agg_query.awaitTermination()

