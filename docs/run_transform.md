## Running the Transform Stage Step-by-Step

The producer is working and sending data to Kafka successfully, here's what to do next to run transform stage:

**Step 5: Run the Transform Stage (Moving Statistics)**

1. Open a new terminal window (keep your producer and Kafka consumer running)
2. Execute the transform stage (moving_stats.py):
```
docker run -it --rm --network=bigdatalab4_default
  -v ${PWD}/src:/app
  -v ${PWD}/spark_checkpoints:/tmp
  bitnami/spark:3.5.0 
  bash -c "pip install pyspark kafka-python && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 /app/transform/moving_stats.py"

docker run -it --rm --network=bigdatalab4_default
  -v ${PWD}/src:/app
  -v ${PWD}/spark_checkpoints:/tmp
  bitnami/spark:3.5.0
  bash -c "pip install pyspark kafka-python &&
  spark-submit
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
  /app/transform/moving_stats.py"

docker run -it --rm --network=bigdatalab4_default 
  -v ${PWD}/src:/app 
  -v ${PWD}/spark_checkpoints:/tmp 
  bitnami/spark:3.5
  bash -c "pip install pyspark kafka-python && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /app/transform/zscore.py"

```

Or:

```
docker-compose exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /app/transform/moving_stats.py
```

*What to expect:*
* You should see Spark initialization logs
* The console output will show the processed data in batches
* Check for any error messages

Step 6: Verify Transform Output
In a new terminal window, check the output topic (btc-price-moving):

```
docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh  
  --bootstrap-server localhost:9092  
  --topic btc-price-moving  
  --from-beginning
```

Expected Output:
You should see JSON messages with the moving statistics for all window sizes, like:

```
{
  "timestamp": "2025-05-03T14:42:00.000Z",
  "symbol": "BTCUSDT",
  "stats": [
    {"window": "30s", "avg_price": 96328.49, "std_price": 0.0},
    {"window": "1m", "avg_price": 96328.49, "std_price": 0.0},
    ...
  ]
}
{
  "timestamp":"2025-05-06 03:56:50",
  "symbol":"BTCUSDT",
  "stats":
  {"window":"30s","avg_price":94442.72499999999,"std_price":0.005345224833707427}}
```