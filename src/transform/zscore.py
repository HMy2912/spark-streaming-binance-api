from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("BTCPriceZScore") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

# Define schemas
price_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("event_time", TimestampType())
])

stats_schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("symbol", StringType()),
    StructField("stats", ArrayType(StructType([
        StructField("window", StringType()),
        StructField("avg_price", DoubleType()),
        StructField("std_price", DoubleType())
    ])))
])

# Read price data from Kafka
price_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(
        from_json(col("value").cast("string"), price_schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withWatermark("event_time", "10 seconds")

# Read stats data from Kafka
stats_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "btc-price-moving") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(
        from_json(col("value").cast("string"), stats_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withWatermark("timestamp", "10 seconds") \
    .select("timestamp", "stats")

# Join and calculate Z-scores
joined_df = price_df.join(
    stats_df,
    expr("""
        event_time = timestamp
    """),
    "inner"
)

# Explode stats array and calculate Z-scores
result_df = joined_df.select(
    col("event_time").alias("timestamp"),
    col("symbol"),
    explode(col("stats")).alias("stat")
).select(
    col("timestamp"),
    col("symbol"),
    col("stat.window").alias("window"),
    ((col("price") - col("stat.avg_price")) / col("stat.std_price")).alias("zscore_price")
)

# Format output
output_df = result_df.groupBy("timestamp", "symbol").agg(
    collect_list(
        struct(
            col("window"),
            col("zscore_price")
        )
    ).alias("zscores")
)

# Write to Kafka
query = output_df.select(
    to_json(struct(
        col("timestamp"),
        col("symbol"),
        col("zscores")
    )).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "btc-price-zscore") \
    .option("checkpointLocation", "/tmp/checkpoint_zscore") \
    .outputMode("append") \
    .start()

query.awaitTermination()