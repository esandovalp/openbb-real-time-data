# kafka_producers/producer_fake.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

tickers = ["AAPL", "MSFT", "TSLA"]
interval = 2  # segundos

# Precios iniciales
state = {
    "AAPL": 180.0,
    "MSFT": 310.0,
    "TSLA": 720.0
}

while True:
    for ticker in tickers:
        # Simular una pequeña variación aleatoria de precio y volumen
        state[ticker] += random.uniform(-1.5, 1.5)
        price = round(state[ticker], 2)
        volume = random.randint(100, 1000)

        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "ticker": ticker,
            "price": price,
            "volume": volume
        }

        print("Sending:", message)
        producer.send("acciones", value=message)

    time.sleep(interval)