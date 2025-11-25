import time
import json
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()

# Fetch API key and other secrets
API_KEY = os.getenv("STOCK_BROKER_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]


producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url=url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error during fetching of symbol {symbol}: {e} ")
        return None


while True:
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            print(f"Producing : {quote}")
            # connect producer to stream the data
            producer.send("stock-quotes",value=quote)
    time.sleep(6)
