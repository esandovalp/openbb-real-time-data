import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

assets = ["bitcoin", "ethereum", "solana"]
interval = 5  # segundos

url = "https://api.coincap.io/v2/assets"

print("Starting CoinCap producer...")

while True:
    try:
        response = requests.get(url, timeout=10)
        data = response.json().get("data", [])

        for asset in data:
            if asset["id"] in assets:
                message = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "ticker": asset["symbol"],
                    "price": float(asset["priceUsd"]),
                    "volume": float(asset["volumeUsd24Hr"])
                }
                print("Sending:", message)
                producer.send("acciones", value=message)

        time.sleep(interval)

    except Exception as e:
        print(f"[ERROR] CoinCap API failed: {e}")
        time.sleep(10)