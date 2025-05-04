import asyncio
import websockets
import json
import os
import dotenv
import psycopg2
import logging


websocket_url = "wss://api.hyperliquid.xyz/ws"

dotenv.load_dotenv()
ENVIRONMENT = os.getenv("ENVIRONMENT")

if ENVIRONMENT == "DEV":
    DB_DATABASE = os.getenv("DEV_DATABASE")
    DB_HOST = os.getenv("DEV_HOST")
    DB_PORT = os.getenv("DEV_PORT")
    DB_USER = os.getenv("DEV_USER")
    DB_PASSWORD = os.getenv("DEV_PASSWORD")
elif ENVIRONMENT == "PROD":
    DB_DATABASE = os.getenv("PROD_DATABASE")
    DB_HOST = os.getenv("PROD_HOST")
    DB_PORT = os.getenv("PROD_PORT")
    DB_USER = os.getenv("PROD_USER")
    DB_PASSWORD = os.getenv("PROD_PASSWORD")
else:
    raise ValueError("Invalid ENVIRONMENT value. Must be 'DEV' or 'PROD'.")


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# Database handler
class DatabaseHandler:
    def __init__(self):
        self.conn = None

    def connect(self):
        if self.conn is None:
            self.conn = psycopg2.connect(dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)

    def insert_candle(self, data):
        pass

    def insert_trade(self, data):
        pass


class WebsocketListener:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    async def listen(self):
        async with websockets.connect(websocket_url) as websocket:
            await websocket.send(json.dumps({"type": "subscribe", "channels": ["candles", "trades"]}))

            while True:
                try:
                    message = await websocket.recv()
                    data = json.loads(message)
                    logger.info(f"Received data: {data}")

                    if data["type"] == "candles":
                        self.db_handler.insert_candle(data)
                    elif data["type"] == "trades":
                        self.db_handler.insert_trade(data)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
