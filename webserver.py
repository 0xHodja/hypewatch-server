from flask import Flask, jsonify
import os
import dotenv
import psycopg2

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


app = Flask(__name__)


class Database:
    def __init__(self):
        self.conn = None

    def db_connect(self):
        if self.conn is not None and self.conn.closed == 0:
            return
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
            except Exception as e:
                raise

    def db_disconnect(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def get_trades(self):
        self.db_connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM trades order by time desc limit 50000")
        trades = cursor.fetchall()
        cursor.close()
        return trades

    def get_candles(self):
        self.db_connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM candles order by t desc limit 10000")
        candles = cursor.fetchall()
        cursor.close()
        return candles


db = Database()


@app.route("/api/trades", methods=["GET"])
def get_trades():
    trades = db.get_trades()
    trades_list = []
    for trade in trades:
        trade_dict = {
            "coin": trade[0],
            "side": trade[1],
            "px": trade[2],
            "sz": trade[3],
            "time": trade[4],
            "hash": trade[5],
            "tid": trade[6],
            "user_buyer": trade[7],
            "user_seller": trade[8],
        }
        trades_list.append(trade_dict)
    return jsonify({"trades": trades_list})


@app.route("/api/candles", methods=["GET"])
def get_candles():
    data = db.get_candles()
    data_list = []
    for row in data:
        row_dict = {
            "t": row[0],
            "s": row[1],
            "i": row[2],
            "o": row[3],
            "c": row[4],
            "h": row[5],
            "l": row[6],
            "v": row[7],
        }
        data_list.append(row_dict)
    return jsonify({"candles": data_list})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
