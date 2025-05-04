# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("BTCPriceMongoLoader") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
#     .config("spark.mongodb.output.uri", "mongodb://admin:admin@mongodb:27017") \
#     .getOrCreate()

# # Define schema
# schema = StructType([
#     StructField("timestamp", TimestampType()),
#     StructField("symbol", StringType()),
#     StructField("zscores", ArrayType(StructType([
#         StructField("window", StringType()),
#         StructField("zscore_price", DoubleType())
#     ])))
# ])

# # Read from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "btc-price-zscore") \
#     .option("startingOffsets", "latest") \
#     .load() \
#     .select(
#         from_json(col("value").cast("string"), schema).alias("data")) \
#     .select("data.*") \
#     .withColumn("timestamp", to_timestamp(col("timestamp"))) \
#     .withWatermark("timestamp", "10 seconds")

# # Explode the array to write to separate collections
# for window in ["30s", "1m", "5m", "15m", "30m", "1h"]:
#     window_df = df.select(
#         col("timestamp"),
#         col("symbol"),
#         expr(f"filter(zscores, x -> x.window = '{window}')[0].zscore_price").alias("zscore_price")
#     ).filter(col("zscore_price").isNotNull())
    
#     query = window_df.writeStream \
#         .foreachBatch(lambda batch_df, batch_id: 
#             batch_df.write \
#                 .format("mongo") \
#                 .mode("append") \
#                 .option("uri", "mongodb://admin:admin@mongodb:27017") \
#                 .option("database", "crypto") \
#                 .option("collection", f"btc-price-zscore{window}") \
#                 .save()
#         ) \
#         .outputMode("append") \
#         .option("checkpointLocation", f"/tmp/checkpoint_mongo_{window}") \
#         .start()

# # Keep the application running
# spark.streams.awaitAnyTermination()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("BTCPriceMongoLoader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Add console output for debugging
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "btc-price-zscore") \
    .load() \
    .selectExpr("CAST(value AS STRING)")  # Add this to see raw data

# Debugging output
console_query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# MongoDB output (updated config)
mongo_query = df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/tmp/checkpoint_mongo") \
    .option("uri", "mongodb://admin:admin@mongodb:27017") \
    .option("database", "crypto") \
    .option("collection", "btc_prices") \
    .start()

console_query.awaitTermination()
mongo_query.awaitTermination()