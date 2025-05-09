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

HEARTBEAT_INTERVAL = 30  # seconds


class WebsocketListener:
    def __init__(self):
        self.conn = None

    def db_connect(self):
        if self.conn is not None and self.conn.closed == 0:
            return
        try:
            self.conn = psycopg2.connect(dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            logger.info("Connected to the database")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    def db_disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def db_insert_candle(self, data):
        candle = data
        try:
            self.db_connect()
            t, s, i, o, c, h, l, v = candle.get("t"), candle.get("s"), candle.get("i"), candle.get("o"), candle.get("c"), candle.get("h"), candle.get("l"), candle.get("v")
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO candles (t, s, i, o, c, h, l, v)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (t, s, i) DO UPDATE SET
                        o = EXCLUDED.o,
                        c = EXCLUDED.c,
                        h = EXCLUDED.h,
                        l = EXCLUDED.l,
                        v = EXCLUDED.v
                    """,
                    (t, s, i, o, c, h, l, v),
                )
                self.conn.commit()
        except Exception as e:
            logger.error(f"Error inserting candle data: {e}")
        finally:
            pass

    def db_insert_trade(self, data):
        for trade in data:
            try:
                self.db_connect()
                coin, side, px, sz, time, hash, tid, user_buyer, user_seller = trade.get("coin"), trade.get("side"), trade.get("px"), trade.get("sz"), int(trade.get("time")), trade.get("hash"), trade.get("tid"), trade.get("users")[0], trade.get("users")[1]
                with self.conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO trades (coin, side, px, sz, time, hash, tid, user_buyer, user_seller)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (tid, coin, time) DO UPDATE SET
                            side = EXCLUDED.side,
                            px = EXCLUDED.px,
                            sz = EXCLUDED.sz,
                            hash = EXCLUDED.hash,
                            user_buyer = EXCLUDED.user_buyer,
                            user_seller = EXCLUDED.user_seller
                        """,
                        (coin, side, px, sz, time, hash, tid, user_buyer, user_seller),
                    )
                    self.conn.commit()
            except Exception as e:
                logger.error(f"Error inserting trade data: {e}")
            finally:
                pass

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
                            try:
                                self.db_insert_trade(trade_data)
                            except Exception as db_exc:
                                logger.error(f"Failed to insert trade into DB: {db_exc}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

                await self.handle_heartbeat(websocket)

    async def listen_candles(self):
        async with websockets.connect(websocket_url) as websocket:
            subscribe_message = {"method": "subscribe", "subscription": {"type": "candle", "coin": "@107", "interval": "1m"}}
            await websocket.send(json.dumps(subscribe_message))

            while True:
                try:
                    response = await websocket.recv()

                    if response == "ping":
                        await websocket.send("pong")
                        continue

                    data = json.loads(response)
                    if data.get("channel") == "candle":
                        candle_data = data.get("data")
                        if candle_data:
                            try:
                                self.db_insert_candle(candle_data)
                            except Exception as db_exc:
                                logger.error(f"Failed to insert trade into DB: {db_exc}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

                await self.handle_heartbeat(websocket)

    async def listen(self):
        try:
            await asyncio.gather(self.listen_trades(), self.listen_candles())
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        except KeyboardInterrupt:
            logger.info("WebSocket connection closed by user.")
        finally:
            self.db_disconnect()
            logger.info("Database connection closed.")


if __name__ == "__main__":
    listener = WebsocketListener()
    asyncio.run(listener.listen())
