import requests
import psycopg2
from datetime import datetime, timedelta
import os
import time
import sys

# === Config ===
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")
FMP_API_KEY = os.environ.get("FMP_API_KEY")

DAYS_BACK = 30
CHUNK_SIZE = 3  # FMP allows ~3 days per 1min request

FMP_URL = "https://financialmodelingprep.com/stable/historical-chart/1min"

# === Helpers ===
def connect():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return psycopg2.connect(conn_string)

def fetch_chunk(symbol, from_date, to_date):
    url = f"{FMP_URL}?symbol={symbol}&from={from_date}&to={to_date}&apikey={FMP_API_KEY}"
    print(f"📡 Fetching: {symbol} {from_date} → {to_date}")
    try:
        res = requests.get(url)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"❌ Fetch failed: {e}")
        return []

def insert_candles(symbol, candles):
    conn = connect()
    cur = conn.cursor()
    inserted = 0

    for row in candles:
        try:
            dt = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S")
            cur.execute("""
                INSERT INTO quant_candles_intraday (ticker, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime) DO NOTHING;
            """, (symbol, dt, row["open"], row["high"], row["low"], row["close"], row["volume"]))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Skipping row due to error: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Inserted {inserted} candles for {symbol}")

# === Main Logic ===
def backfill(symbol):
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=DAYS_BACK)
    current = start_date

    while current < today:
        from_str = current.strftime("%Y-%m-%d")
        to_str = (current + timedelta(days=CHUNK_SIZE - 1)).strftime("%Y-%m-%d")

        candles = fetch_chunk(symbol, from_str, to_str)
        if candles:
            insert_candles(symbol, candles)
        else:
            print(f"⚠️ No data returned for {symbol} {from_str} → {to_str}")

        current += timedelta(days=CHUNK_SIZE)
        time.sleep(1)  # avoid rate limits

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        print(f"🔍 Backfilling data for: {ticker}")
        backfill(ticker)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_backfill_intraday.py GRRR")
