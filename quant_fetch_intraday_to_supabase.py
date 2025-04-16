import requests
import psycopg2
from datetime import datetime, timezone
import os
import sys

# === PostgreSQL Connection Credentials (Render + Supabase) ===
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "3elJBVtbBkZ1YEdr")
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")
DB_PORT = os.environ.get("DB_PORT", "5432")

# === FMP Config ===
FMP_API_KEY = os.environ.get("FMP_API_KEY", "E1x0AcpDC2qVyv4ebf2W9Wjge9EemKGw")
FMP_CANDLES_URL = "https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}&apikey={apikey}"

def fetch_intraday_candles(symbol):
    url = FMP_CANDLES_URL.format(symbol=symbol, apikey=FMP_API_KEY)
    print(f"📡 Fetching 1-min candles for: {symbol}...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        candles = response.json()

        if not isinstance(candles, list):
            print(f"⚠️ Unexpected response format: {candles}")
            return []

        return candles

    except requests.exceptions.RequestException as e:
        print(f"❌ API Error: {e}")
        return []

def store_candles_in_supabase(symbol, candles):
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        for candle in candles:
            candle_dt = datetime.strptime(candle["date"], "%Y-%m-%d %H:%M:%S")
            open_price = candle["open"]
            high_price = candle["high"]
            low_price = candle["low"]
            close_price = candle["close"]
            volume = candle["volume"]

            cur.execute("""
                INSERT INTO quant_candles_intraday (ticker, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime) DO NOTHING;
            """, (symbol, candle_dt, open_price, high_price, low_price, close_price, volume))

        conn.commit()
        conn.close()
        print(f"✅ Stored {len(candles)} candles for {symbol}.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

def fetch_and_store_candles(symbol):
    candles = fetch_intraday_candles(symbol)
    if candles:
        store_candles_in_supabase(symbol, candles)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "GRRR"
        print(f"⚠️ No ticker provided, using default: {symbol}")

    fetch_and_store_candles(symbol)
