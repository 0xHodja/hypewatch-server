# %%

import asyncio
import websockets
import json
import os
import dotenv
import psycopg2
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


data_folder = os.path.dirname(__file__, "./temp_data")

WS_URL = "wss://api.hyperliquid.xyz/ws"


async def listen():
    try:
        if not os.path.exists(data_folder):
            os.makedirs(data_folder)
        output_file = os.path.join(data_folder, "responses.txt")
        async with websockets.connect(WS_URL, ping_interval=None, ping_timeout=None) as websocket:
            subscribe_message = {"method": "subscribe", "subscription": {"type": "trades", "coin": "@107"}}
            await websocket.send(json.dumps(subscribe_message))

            while True:
                response = await websocket.recv()

                # if channel == "trades":
                #     data = json.loads(response)
                #     logger.info(f"Received trade data: {data}")

                with open(output_file, "a") as file:
                    file.write(response + "\n")
                data = json.loads(response)
                logger.info(f"Received data: {data}")

    except Exception as e:
        logger.error(f"Error connecting to WebSocket: {e}")
        return

    except KeyboardInterrupt:
        logger.info("WebSocket connection closed by user.")
        return

    finally:
        if websocket:
            await websocket.close()
            logger.info("WebSocket connection closed.")


if __name__ == "__main__":
    asyncio.run(listen())
