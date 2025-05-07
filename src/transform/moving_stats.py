from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("BTCPriceMovingStats") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

def process_stream(spark):
    # Kafka stream input
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "btc-price") \
        .option("startingOffsets", "latest") \
        .load()

    # Schema matches producer
    schema = StructType([
        StructField("symbol", StringType()),
        StructField("price", StringType()),
        StructField("timestamp", StringType())
    ])

    # Parse and cast input with watermark
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            col("data.price").cast(DoubleType()).alias("price"),
            to_timestamp(col("data.timestamp")).alias("timestamp")
        ) \
        .withWatermark("timestamp", "10 seconds")

    # Define window specifications
    window_specs = [
        ("30 seconds", "10 seconds", "30s"),
        ("1 minute", "20 seconds", "1m"),
        ("5 minutes", "1 minute", "5m"),
        ("15 minutes", "3 minutes", "15m"),
        ("30 minutes", "5 minutes", "30m"),
        ("1 hour", "10 minutes", "1h")
    ]

    # Calculate statistics for each window configuration
    window_dfs = []
    for duration, slide, label in window_specs:
        windowed = parsed_df \
            .withWatermark("timestamp", "10 seconds") \
            .groupBy(
                window(col("timestamp"), duration, slide),
                col("symbol")
            ) \
            .agg(
                avg("price").alias("avg_price"),
                stddev("price").alias("std_price")
            ) \
            .select(
                col("window.end").alias("timestamp"),
                col("symbol"),
                lit(label).alias("window"),
                col("avg_price"),
                col("std_price")
            )
        window_dfs.append(windowed)

    # Combine all window results
    combined_df = window_dfs[0]
    for df in window_dfs[1:]:
        combined_df = combined_df.union(df)

    # Format output
    output_df = combined_df \
        .groupBy("timestamp", "symbol") \
        .agg(collect_list(
            struct(
                col("window"),
                col("avg_price"),
                col("std_price")
            )
        ).alias("stats")) \
        .select(
            to_json(struct(
                date_format(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("timestamp"),
                col("symbol"),
                col("stats")
            )).alias("value")
        )

    # Debugging output to console
    debug_query = output_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Write to Kafka with append mode
    query = output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "btc-price-moving") \
        .option("checkpointLocation", "/tmp/checkpoints/moving_stats") \
        .outputMode("update") \
        .start()
        
    return query, debug_query

def main():
    spark = None
    try:
        spark = create_spark_session()
        query, debug_query = process_stream(spark)
        query.awaitTermination()
        debug_query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping...")
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()