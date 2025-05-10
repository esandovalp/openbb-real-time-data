import yfinance as yf
from kafka import KafkaProducer
import json
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

tickers = ["AAPL", "MSFT", "TSLA"]
interval = 30  # segundos entre rondas

def get_latest_price(ticker):
    try:
        data = yf.Ticker(ticker).history(period="1d")
        if not data.empty:
            last_row = data.tail(1).reset_index().iloc[0]
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "ticker": ticker,
                "price": round(last_row["Close"], 2),
                "volume": int(last_row["Volume"])
            }
    except Exception as e:
        print(f"[WARN] {ticker}: {e}")
    return None

print(f"Starting producer_yfinance.py with tickers: {tickers}")
while True:
    for ticker in tickers:
        message = get_latest_price(ticker)
        if message:
            print("Sending:", message)
            producer.send("acciones", value=message)
    time.sleep(interval)