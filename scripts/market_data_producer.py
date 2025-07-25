#!/usr/bin/env python3
import os
import yfinance as yf
from kafka import KafkaProducer
import json
import time
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = "localhost:9092" # Use 'broker:29092' for Docker internal network
KAFKA_TOPIC = "raw_market_ohlcv"
BANK_SYMBOLS = ["JPM", "BAC", "C", "WFC", "GS"]
FETCH_INTERVAL_SECONDS = 60 # Fetch data every 60 seconds (1 minute)

# Initialize Kafka Producer
# value_serializer converts Python dict to JSON bytes for Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',  # Ensure all brokers acknowledge the message
    retries=3    # Retry sending on failure
)

def produce_bank_prices_to_kafka(symbols: list):
    current_time = datetime.now().isoformat()
    print(f"[{current_time}] Fetching data and producing to Kafka topic: {KAFKA_TOPIC}")

    for sym in BANK_SYMBOLS:
        try:
            # Fetches minute-level data for the last day
            df = yf.download(sym, period='1d', interval='1m', progress=False)

            if not df.empty:
                # Option 1: Convert each row to a dictionary and send individually
                for index, row in df.iterrows():
                    # Create a dictionary for the data point
                    # Explicitly convert numeric types to standard Python floats
                    data_to_send = {
                        "symbol": sym,
                        "timestamp": index.isoformat(), # Convert timestamp to ISO format string
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume']) # Volume is usually an integer
                    }
                    producer.send(KAFKA_TOPIC, data_to_send)
                    # print(f"Sent {sym} data point for {index.isoformat()}") # Optional: for debugging
                producer.flush() # Ensure all messages are sent for this symbol
            else:
                print(f"No data fetched for {sym} or DataFrame was empty.")

        except Exception as e:
            print(f"Error processing {sym}: {e}")
            


if __name__ == '__main__':
    print(f"Starting real-time market data producer. Sending data for: {', '.join(BANK_SYMBOLS)}")
    print(f"Fetching every {FETCH_INTERVAL_SECONDS} seconds.")
    try:
        while True:
            produce_bank_prices_to_kafka(BANK_SYMBOLS)
            time.sleep(FETCH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush() # Ensure all messages are sent before exiting
        producer.close()
        print("Kafka producer closed.")