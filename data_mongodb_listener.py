import os
import dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import websockets
import json
import asyncio
import logging


dotenv.load_dotenv()

MONGODB_USER = os.getenv("PROD_MONGODB_USER")
MONGODB_PASSWORD = os.getenv("PROD_MONGODB_PASSWORD")
MONGODB_APPNAME = os.getenv("PROD_MONGODB_APPNAME")
MONGODB_CLUSTER = os.getenv("PROD_MONGODB_CLUSTER")

db_uri = f"mongodb+srv://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_CLUSTER}.wrgo3pa.mongodb.net/?retryWrites=true&w=majority&appName={MONGODB_APPNAME}"
websocket_url = "wss://api.hyperliquid.xyz/ws"

logger = logging.getLogger(__name__)


class WebsocketListener:
    def __init__(self):
        self.dbclient = MongoClient(db_uri, server_api=ServerApi("1"))

    async def handle_heartbeat(self, websocket):
        """Handle client-side heartbeat"""
        current_time = asyncio.get_event_loop().time()
        if not hasattr(websocket, "last_heartbeat"):
            websocket.last_heartbeat = current_time
        elif current_time - websocket.last_heartbeat >= 15:  # Reduced to 15 seconds
            heartbeat_message = {"method": "ping"}
            await websocket.send(json.dumps(heartbeat_message))
            websocket.last_heartbeat = current_time

    async def listen_trades(self):
        async with websockets.connect(websocket_url) as websocket:
            subscribe_message = {"method": "subscribe", "subscription": {"type": "trades", "coin": "@107"}}
            await websocket.send(json.dumps(subscribe_message))

            while True:
                try:
                    response = await websocket.recv()

                    if response == "ping":
                        await websocket.send("pong")
                        continue

                    data = json.loads(response)
                    if data.get("channel") == "trades":
                        trade_data = data.get("data")
                        if trade_data:
                            for trade in trade_data:
                                self.dbclient.hypewatch.trades.update_one({"tid": trade["tid"]}, {"$set": trade}, upsert=True)
                                print("Inserted trade: ", trade)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

                await self.handle_heartbeat(websocket)

    async def listen_candles(self):
        pass

    async def listen(self):
        await asyncio.gather(self.listen_trades(), self.listen_candles())


if __name__ == "__main__":
    listener = WebsocketListener()
    asyncio.run(listener.listen())
