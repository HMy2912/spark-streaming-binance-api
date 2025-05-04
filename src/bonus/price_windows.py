from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder \
    .appName("BTCPriceWindows") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("event_time", TimestampType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(
        from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .withWatermark("event_time", "10 seconds")

# Create a view for self-join
df.createOrReplaceTempView("prices")

# Find higher and lower prices within 20 seconds
query = spark.sql("""
    SELECT 
        p1.event_time as timestamp,
        p1.price as original_price,
        MIN(CASE WHEN p2.price > p1.price THEN p2.event_time END) as higher_time,
        MIN(CASE WHEN p2.price < p1.price THEN p2.event_time END) as lower_time
    FROM prices p1
    LEFT JOIN prices p2
    ON p2.event_time > p1.event_time AND p2.event_time <= p1.event_time + INTERVAL 20 seconds
    GROUP BY p1.event_time, p1.price
""")

# Calculate time differences
result_df = query.select(
    col("timestamp"),
    (coalesce(
        unix_timestamp(col("higher_time")) - unix_timestamp(col("timestamp")),
        lit(20.0)
    ).alias("higher_window")
    ),
    (coalesce(
        unix_timestamp(col("lower_time")) - unix_timestamp(col("timestamp")),
        lit(20.0)
    ).alias("lower_window")
    )
)

# Write higher prices to Kafka
higher_query = result_df.select(
    col("timestamp"),
    col("higher_window")
).select(
    to_json(struct(
        col("timestamp"),
        col("higher_window")
    )).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "btc-price-higher") \
    .option("checkpointLocation", "/tmp/checkpoint_higher") \
    .outputMode("append") \
    .start()

# Write lower prices to Kafka
lower_query = result_df.select(
    col("timestamp"),
    col("lower_window")
).select(
    to_json(struct(
        col("timestamp"),
        col("lower_window")
    )).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "btc-price-lower") \
    .option("checkpointLocation", "/tmp/checkpoint_lower") \
    .outputMode("append") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()