#!/usr/bin/env python3
import logging
import os
import json
import time
from datetime import datetime

# --- Logging Setup ---
# Configure logging immediately to capture everything from the very start.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='producer.log',
    filemode='w'
)

logging.info("Logger configured. Starting script execution.")

try:
    import yfinance as yf
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    logging.info("Successfully imported yfinance and kafka libraries.")
except ImportError as e:
    logging.error(f"Failed to import required libraries: {e}")
    exit(1)

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw_market_ohlcv"
BANK_SYMBOLS = ["JPM", "BAC", "C", "WFC", "GS"]
FETCH_INTERVAL_SECONDS = 10  # Fetch data every 10 seconds for faster debugging

# --- Kafka Producer Initialization ---
producer = None
logging.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3,
        request_timeout_ms=15000  # Set a 15-second timeout to avoid long hangs
    )
    logging.info("Kafka producer initialized successfully.")
except NoBrokersAvailable:
    logging.error(f"Fatal: Could not find any brokers at {KAFKA_BROKER}. Please ensure Kafka is running and accessible.")
    exit(1)
except Exception as e:
    logging.error(f"Fatal: An unexpected error occurred during Kafka producer initialization: {e}")
    exit(1)

def produce_bank_prices_to_kafka(symbols: list):
    """Fetches data for a list of symbols and sends it to Kafka."""
    logging.info(f"Starting new fetch cycle for symbols: {', '.join(symbols)}")
    for sym in symbols:
        try:
            df = yf.download(sym, period='1d', interval='1m', progress=False)
            if not df.empty:
                for index, row in df.iterrows():
                    data_to_send = {
                        "symbol": sym,
                        "timestamp": index.isoformat(),
                        "open": float(row['Open']),
                        "high": float(row['High']),
                        "low": float(row['Low']),
                        "close": float(row['Close']),
                        "volume": int(row['Volume'])
                    }
                    producer.send(KAFKA_TOPIC, data_to_send)
                producer.flush()
                logging.info(f"Successfully produced {len(df)} messages for symbol {sym}.")
            else:
                logging.warning(f"No data fetched for {sym}; the DataFrame was empty.")
        except Exception as e:
            logging.error(f"Error processing symbol {sym}: {e}", exc_info=True)

if __name__ == '__main__':
    logging.info(f"Starting real-time market data producer for topic '{KAFKA_TOPIC}'.")
    try:
        while True:
            produce_bank_prices_to_kafka(BANK_SYMBOLS)
            logging.info(f"Cycle complete. Waiting {FETCH_INTERVAL_SECONDS} seconds.")
            time.sleep(FETCH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logging.info("Producer stopped by user.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer flushed, closed, and script finished.")