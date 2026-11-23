import os
from pyspark.sql import SparkSession, DataFrame
KAFKA_HOST = os.environ.get('KAFKA_HOST')
KAFKA_TOPIC= os.environ.get('KAFKA_TOPIC')
spark: SparkSession = SparkSession.builder.appName("Stream_Processor").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
kafka_df=(spark.readStream.format("kafka").
          option("kafka.bootstrap.servers", KAFKA_HOST).
          option("subscribe", KAFKA_TOPIC).option("startingOffsets", "earliest"))


query =kafka_df.format("rate").load().writeStream.format("console").option("startingOffsets", "earliest").start().awaitTermination()


