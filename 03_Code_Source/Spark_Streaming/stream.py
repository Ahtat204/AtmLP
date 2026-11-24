import os
from pyspark.sql import SparkSession

KAFKA_HOST = os.environ.get("KAFKA_HOST")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")

# 1. Verify environment variables are actually loaded
assert KAFKA_HOST, "KAFKA_HOST is not set"
assert KAFKA_TOPIC, "KAFKA_TOPIC is not set"
print(f"Connecting to {KAFKA_HOST} | Topic: {KAFKA_TOPIC}")

spark = SparkSession.builder.appName("Stream_Processor").getOrCreate()

# 2. Keep WARN level temporarily so Kafka connection/topic issues show up
spark.sparkContext.setLogLevel("WARN")

# 3. Call .load() here to get an actual DataFrame
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOST)
    .option("subscribePattern", f"^{KAFKA_TOPIC}$")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Cast binary key/value to readable strings
# readable_df = kafka_df.selectExpr(
#     "CAST(key AS STRING)",
#     "CAST(value AS STRING)",
#     "topic",
#     "partition",
#     "offset"
# )
readable_df = kafka_df.selectExpr(
    "CAST(value AS STRING)",
)

# 5. Start the console stream
# query = (
#     readable_df.writeStream.outputMode("append")
#     .format("console")
#     .option("truncate", False)
#     .option("truncate", "false")
#     .option("numRows", 20)
#     .start()
# )
query = (
    readable_df.writeStream.outputMode("append")
    .format("parquet")
     .outputMode("append")
     .option("path", "/app/parquets/events")
    .option("checkpointLocation", "/app/checkpoints")
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
