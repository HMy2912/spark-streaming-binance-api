import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = "btc-price"
BINANCE_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

while True:
    try:
        response = requests.get(BINANCE_URL)
        data = response.json()

        if "symbol" in data and "price" in data:
            price_info = {
                "symbol": data["symbol"],
                "price": float(data["price"]),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }
            print(f"Sending: {price_info}")
            producer.send(KAFKA_TOPIC, price_info)
        
        time.sleep(0.1)  # 100ms
    except Exception as e:
        print(f"Error: {e}")
