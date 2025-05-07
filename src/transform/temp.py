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
        .withWatermark("timestamp", "30 seconds")

    # Define all required window configurations
    window_specs = [
        {"label": "30s", "duration": "30 seconds", "slide": "10 seconds"},
        {"label": "1m", "duration": "1 minute", "slide": "10 seconds"},
        {"label": "5m", "duration": "5 minutes", "slide": "30 seconds"},
        {"label": "15m", "duration": "15 minutes", "slide": "1 minute"},
        {"label": "30m", "duration": "30 minutes", "slide": "5 minutes"},
        {"label": "1h", "duration": "1 hour", "slide": "10 minutes"}
    ]

    # Process each window configuration separately
    queries = []
    for spec in window_specs:
        windowed_df = parsed_df.groupBy(
            window(col("timestamp"), spec["duration"], spec["slide"]),
            col("symbol")
        ).agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        ).select(
            col("window.end").cast("string").alias("timestamp"),
            col("symbol"),
            lit(spec["label"]).alias("window"),
            col("avg_price"),
            col("std_price")
        )

        # Create array of stats for the required output format
        stats_df = windowed_df.groupBy("timestamp", "symbol").agg(
            collect_list(
                struct(
                    col("window"),
                    col("avg_price"),
                    col("std_price")
                )
            ).alias("stats")
        )

        # Format output to match requirements
        output_df = stats_df.select(
            to_json(struct(
                col("timestamp"),
                col("symbol"),
                col("stats")
            )).alias("value")
        )

        # Write each window's results to Kafka with complete mode
        query = output_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "btc-price-moving") \
            .option("checkpointLocation", f"/tmp/checkpoints/moving_stats_{spec['label']}") \
            .outputMode("complete") \
            .start()
            
        queries.append(query)

    return queries

def main():
    spark = None
    try:
        spark = create_spark_session()
        queries = process_stream(spark)
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping...")
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()