from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, floor
from pyspark.sql.types import StructType, StringType

schema = StructType().add("client_id", StringType()).add("emoji", StringType()).add("timestamp", StringType())

spark = SparkSession.builder.appName("EmojiConsumer").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "emoji-topic").load()

parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

parsed_df = parsed_df.drop("timestamp")

aggregated_df = parsed_df.groupBy(col("emoji")).agg(count("*").alias("emoji_count"))

aggregated_df_compressed = aggregated_df.withColumn("compressed_count", floor(col("emoji_count") / 10))

query = aggregated_df_compressed.writeStream.outputMode("complete").format("console").trigger(processingTime='10 seconds').start()

query.awaitTermination()

