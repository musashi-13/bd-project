from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StringType
from tabulate import tabulate

spark = SparkSession.builder \
    .appName("EmojiProcessor") \
    .master("local[*]") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.memory.fraction", "0.9") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

KAFKA_BROKER = "localhost:9092"
TOPIC = "emoji-topic"
CONSUMER_GROUP = "emoji-stream"

schema = StructType() \
    .add("user_id", StringType()) \
    .add("emoji", StringType()) \
    .add("timestamp", StringType())

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("kafka.group.id", CONSUMER_GROUP) \
    .option("maxOffsetsPerTrigger", 2000) \
    .load()

parsed_stream = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def process_batch(df, batch_id):
    if not df.rdd.isEmpty():
        emoji_counts = df.groupBy("emoji").agg(count("*").alias("count"))
        results = emoji_counts.collect()
        
        if results:
            headers = ["Emoji", "Count"]
            table = tabulate(results, headers=headers, tablefmt="grid")
            print(f"Processing batch {batch_id}")
            print(table)
            print(f"Total records in this batch: {df.count()}")

query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .trigger(processingTime="2 seconds") \
    .start()

query.awaitTermination()