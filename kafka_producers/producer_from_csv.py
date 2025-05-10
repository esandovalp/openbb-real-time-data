import csv
import json
import time
from kafka import KafkaProducer
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

# Ruta al archivo con datos históricos de acciones (puedes usar datos reales)
csv_path = "kafka_producers/sample_prices.csv"

with open(csv_path, "r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "ticker": row["ticker"],
            "price": float(row["price"]),
            "volume": int(row["volume"])
        }
        print("Sending:", message)
        producer.send("acciones", value=message)
        time.sleep(1)