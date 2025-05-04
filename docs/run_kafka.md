## Running the Kafka Producer Step-by-Step

Here's how to execute and verify your Kafka producer:

**Step 1: Start Your Docker Services**

First, ensure all your Docker services are running. Open your terminal or PowerShell and execute:

```powershell
docker-compose up -d
```

**Step 2: Run the Producer**

Execute the following command to start your producer. This command assumes your producer script (`producer.py`) is located in a `src/extract` directory relative to your current working directory.

```powershell
docker run -it --rm --network=bigdatalab4_default `
  -v ${PWD}/src:/app `
  python:3.9 bash -c "pip install kafka-python requests && python /app/extract/producer.py"
```

**Step 3: Verify Producer Output**

You should observe continuous output in the producer's terminal, similar to this:

```powershell
Sent: {'symbol': 'BTCUSDT', 'price': '96328.49000000', 'event_time': '2025-05-03T14:41:57.028183Z'}
Sent: {'symbol': 'BTCUSDT', 'price': '96328.49000000', 'event_time': '2025-05-03T14:41:57.264687Z'}
```

This indicates that your producer is successfully sending messages.

**Step 4: Verify Messages in Kafka**

Open a new PowerShell or terminal window to check if the messages are being received by Kafka:

```
docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 `
  --topic btc-price `
  --from-beginning
```

Note:

* Replace `<your_kafka_service_name>` with the name of your Kafka service as defined in your `docker-compose.yml` (e.g., `kafka`).
* Replace `<your_kafka_topic_name>` with the name of the Kafka topic your producer is sending messages to (e.g., `btc-price`).

If you see output similar to the producer's output, your messages are successfully reaching Kafka.

**If You Don't See Messages:**

Check Kafka status:

```
docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

(This should show `btc-price` in the list.)

Check producer logs for errors: Examine the output of the producer terminal for any error messages.

**Test connectivity between containers:**

```
docker run --rm -it --network=bigdatalab4_default alpine ping kafka
docker run --rm -it --network=bigdatalab4_default alpine telnet kafka 9092
```

Successful `ping` and `telnet` indicate network connectivity.

*To Stop the Producer:*

Press Ctrl+C in the producer terminal window.

*To Monitor Continuously:*

Open three separate terminal windows:

1. Producer: Run your Python producer script.
2. Kafka Consumer: Run the Kafka console consumer command to see incoming messages.
3. Kafka Logs: Monitor the Kafka broker activity:

```
docker-compose logs -f kafka
```