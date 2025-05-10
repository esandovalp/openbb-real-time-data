# kafka_producers/producer_yfinance.py
import yfinance as yf
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

tickers = ['AAPL', 'TSLA', 'MSFT']
interval = 5  # cada 5 segundos

while True:
    for ticker in tickers:
        data = yf.Ticker(ticker).history(period="1d", interval="1m")
        if data.empty:
            continue
        last_row = data.tail(1).reset_index().iloc[0]
        message = {
            'timestamp': str(last_row['Datetime']),
            'ticker': ticker,
            'price': round(last_row['Close'], 2),
            'volume': int(last_row['Volume'])
        }
        print(f"Sending: {message}")
        producer.send('acciones', value=message)
    time.sleep(interval)
