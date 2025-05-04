import requests
import json
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    api_version=(3, 0, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol='PLAINTEXT'
)

API_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

def fetch_and_publish():
    while True:
        try:
            response = requests.get(API_URL)
            data = response.json()
            data['event_time'] = datetime.utcnow().isoformat() + 'Z'
            
            if 'symbol' in data and 'price' in data:
                producer.send('btc-price', value=data)
                print(f"Sent: {data}")
            else:
                print("Invalid data format received")
                
        except Exception as e:
            print(f"Error: {e}")
        
        time.sleep(0.1)

if __name__ == "__main__":
    fetch_and_publish()