from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

try:
    # Create Spark session
    spark = SparkSession.builder.appName("BTCPriceMovingStats") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    # Define schema for incoming data
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
        .load()

    # Parse JSON and extract fields
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("event_time", to_timestamp(col("event_time"))) \
        .withWatermark("event_time", "10 seconds")

    # Define window durations
    window_durations = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]

    # Calculate stats for each window
    results = []
    for duration in window_durations:
        windowed = parsed_df.groupBy(
            window(col("event_time"), duration),  
            col("symbol") 
        ).agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        )
        
        # Format results
        result = windowed.select(
            col("window.end").cast("string").alias("timestamp"),
            col("symbol"),
            struct(
                lit(duration).alias("window"),
                col("avg_price"),
                col("std_price")
            ).alias("stats")
        )
        results.append(result)

    # Union all results
    final_df = results[0]
    for df in results[1:]:
        final_df = final_df.union(df)

    # Write to Kafka
    query = final_df.select(
        to_json(struct(
            col("timestamp"),
            col("symbol"),
            col("stats")
        )).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "btc-price-moving") \
        .option("checkpointLocation", "/tmp/checkpoint_moving") \
        .outputMode("append") \
        .start()

    query.awaitTermination()
except Exception as e:
    print(f"Error occurred: {str(e)}")
    raise