from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, to_timestamp, expr, udf
from pyspark.sql.types import *
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("BTCPriceZScore") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

def process_stream(spark):
    # Define schemas
    price_schema = StructType([
        StructField("symbol", StringType(), nullable=False),
        StructField("price", StringType(), nullable=False),
        StructField("event_time", StringType(), nullable=False)
    ])
    
    stats_schema = StructType([
        StructField("window", StringType(), nullable=False),
        StructField("avg_price", DoubleType(), nullable=False),
        StructField("std_price", DoubleType(), nullable=False)
    ])
    
    moving_stats_schema = StructType([
        StructField("timestamp", StringType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("stats", ArrayType(stats_schema), nullable=True)
    ])

    # Read price stream
    price_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "btc-price") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), price_schema).alias("data")) \
        .select(
            col("data.symbol").alias("symbol"),
            col("data.price").cast(DoubleType()).alias("price"),
            to_timestamp(col("data.event_time")).alias("event_time")
        ) \
        .withWatermark("event_time", "30 seconds")

    # Read moving stats stream
    moving_stats_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "btc-price-moving") \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), moving_stats_schema).alias("data")) \
        .select(
            to_timestamp(col("data.timestamp")).alias("stats_time"),
            col("data.symbol").alias("symbol"),
            col("data.stats").alias("stats")
        ) \
        .withWatermark("stats_time", "30 seconds")

    # UDF for z-score calculation
    def calculate_zscores(price, stats):
        results = []
        if not stats:
            return results
        try:
            for stat in stats:
                std = stat.get('std_price', 0)
                avg = stat.get('avg_price', 0)
                win = stat.get('window', "unknown")
                z = 0.0 if std == 0 else (price - avg) / std
                results.append({
                    "window": win,
                    "zscore_price": z
                })
        except Exception as e:
            pass  # In production, consider logging this
        return results

    calculate_zscores_udf = udf(
        calculate_zscores,
        ArrayType(StructType([
            StructField("window", StringType(), nullable=False),
            StructField("zscore_price", DoubleType(), nullable=False)
        ]))
    )

    # Join with time range condition
    joined_df = price_df.alias("price").join(
        moving_stats_df.alias("stats"),
        expr("""
            price.symbol = stats.symbol AND
            price.event_time >= stats.stats_time AND
            price.event_time <= stats.stats_time + interval 30 seconds
        """),
        "inner"
    )

    # Calculate z-scores
    zscore_df = joined_df.withColumn(
        "zscores",
        calculate_zscores_udf(col("price.price"), col("stats.stats"))
    ).select(
        col("price.event_time").alias("timestamp"),
        col("price.symbol").alias("symbol"),
        col("zscores")
    )

    # Write to Kafka
    query = zscore_df.select(
        to_json(struct(
            col("timestamp").cast("string").alias("timestamp"),
            col("symbol"),
            col("zscores")
        )).alias("value")
    ).writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "btc-price-zscore") \
        .option("checkpointLocation", "/tmp/checkpoints/zscore") \
        .outputMode("append") \
        .start()

    return query

def main():
    spark = None
    try:
        spark = create_spark_session()
        query = process_stream(spark)
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping gracefully...")
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        raise
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
