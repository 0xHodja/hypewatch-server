# %%
import time
import pandas as pd
from hyperliquid.info import Info
from hyperliquid.utils import constants
from dotenv import load_dotenv
import os
import psycopg2

from datetime import datetime, timedelta
import json
import requests

# This script fetches the latest 48 hours of 1-minute candle data for the HYPE/USDC trading pair from the Hyperliquid API.

# prepare the environment variables
load_dotenv()

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

DB_TABLE = "candles"
COINAPI_API_KEY = os.getenv("COINAPI_API_KEY")
data_folder = os.path.join(os.path.dirname(__file__), "historical_data")


# download data from coin api
def download_data_from_coin_api():
    if not COINAPI_API_KEY:
        raise ValueError("COINAPI_API_KEY not found in environment variables.")

    start_date = datetime(2025, 4, 28)
    end_date = datetime(2025, 5, 2)

    current_date = start_date
    while current_date <= end_date:
        time_start = current_date.isoformat()
        time_start_dt = datetime.fromisoformat(time_start)
        time_end_dt = time_start_dt + timedelta(days=1)
        time_end = time_end_dt.isoformat()
        url = f"https://rest.coinapi.io/v1/ohlcv/HYPERLIQUID_SPOT_HYPE_USDC/history?period_id=1MIN&time_start={time_start}&time_end={time_end}&limit=10000"

        payload = {}
        headers = {"Accept": "text/plain", "X-CoinAPI-Key": COINAPI_API_KEY}

        response = requests.request("GET", url, headers=headers, data=payload)

        with open(f"{data_folder}/{current_date.date()}.txt", "w") as file:
            file.write(response.text)

        print(time_start)
        current_date += timedelta(days=1)


def read_data_from_historical_data():
    # List all JSON files in the folder
    json_files = [f for f in os.listdir(data_folder) if f.endswith(".txt")]
    data_list = []

    # Read each JSON file and append its data to the list
    for file in json_files:
        file_path = os.path.join(data_folder, file)
        with open(file_path, "r") as f:
            records = json.load(f)
            data_list.extend(records)

    df = pd.DataFrame(data_list)
    df = df.drop(columns=["time_open", "time_period_end", "time_close"], errors="ignore")

    df["s"] = "@107"
    df["i"] = "1m"

    # Convert "t" (time_open) to a Unix timestamp in milliseconds
    df["time_period_start"] = pd.to_datetime(df["time_period_start"]).astype(int) // 10**6
    df = df.rename(columns={"time_period_start": "t", "price_open": "o", "price_close": "c", "price_high": "h", "price_low": "l", "volume_traded": "v"})
    return df


def request_hyperliquid_candles():
    # request the last 48 hours of 1m candles for HYPE/USDC
    current_time = int(time.time() * 1000)
    start_time = current_time - (48 * 60 * 60 * 1000)

    info = Info(constants.MAINNET_API_URL, skip_ws=True)
    res = info.candles_snapshot("HYPE/USDC", "1m", start_time, current_time)

    df = pd.DataFrame(res, columns=["t", "T", "s", "i", "o", "c", "h", "l", "v"])
    return df


def write_df_candles_to_database(df):
    # Insert the OHLC data into the database
    df_to_insert = df[["t", "s", "i", "o", "c", "h", "l", "v"]]

    # Establish a connection to the database
    try:
        conn = psycopg2.connect(dbname=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        cursor = conn.cursor()

        print("Connected to the database")

        # Insert data row by row
        insert_query = f"""
        INSERT INTO {DB_TABLE} (t, s, i, o, c, h, l, v)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (t, s, i)
        DO UPDATE SET
            o = EXCLUDED.o,
            c = EXCLUDED.c,
            h = EXCLUDED.h,
            l = EXCLUDED.l,
            v = EXCLUDED.v;
        """
        row_values = df_to_insert[["t", "s", "i", "o", "c", "h", "l", "v"]].values.tolist()

        for i, value in enumerate(row_values):
            cursor.execute(insert_query, value)
            if i % 100 == 0:
                print(f"Inserted {i} rows so far... out of {len(row_values)}")
                conn.commit()

        # Commit the transaction
        conn.commit()
        print(f"Inserted {len(df_to_insert)} rows into the '{DB_TABLE}' table.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        # Close the connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Connection closed")


def export_df_to_csv(df, filename="data_for_upload.csv"):
    try:
        export_path = os.path.join(os.path.dirname(__file__), filename)
        df.to_csv(export_path, index=False, header=False)
        print(f"DataFrame exported to {export_path}")
    except Exception as e:
        print(f"An error occurred while exporting to CSV: {e}")


# download_data_from_coin_api()

df = read_data_from_historical_data()
# write_df_candles_to_database(df)
export_df_to_csv(df[["t", "s", "i", "o", "c", "h", "l", "v"]], "candles_historical_data.csv")

df = request_hyperliquid_candles()
write_df_candles_to_database(df)
