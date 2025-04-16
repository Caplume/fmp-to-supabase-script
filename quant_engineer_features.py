import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
import os
import time
import sys
from ta.momentum import RSIIndicator
from ta.trend import MACD, SMAIndicator

# === Config ===
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")

# === Helpers ===
def connect():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return psycopg2.connect(conn_string)

def compute_indicators(df):
    df = df.copy()
    df["rsi"] = RSIIndicator(close=df["close"], window=14).rsi()
    df["vwap"] = (df["volume"] * (df["high"] + df["low"] + df["close"]) / 3).cumsum() / df["volume"].cumsum()

    macd = MACD(close=df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()

    df["ma_50"] = SMAIndicator(close=df["close"], window=50).sma_indicator()
    df["ma_200"] = SMAIndicator(close=df["close"], window=200).sma_indicator()

    df["is_uptrend"] = df["close"] > df["ma_50"]
    df["rel_volume"] = df["volume"] / df["volume"].rolling(window=20).mean()

    return df

def store_features(symbol, df):
    conn = connect()
    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO quant_features_intraday (
                    ticker, datetime, rsi, vwap, macd, macd_signal, ma_50, ma_200, rel_volume, is_uptrend
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime) DO NOTHING;
            """, (
                symbol, row["datetime"], row["rsi"], row["vwap"], row["macd"],
                row["macd_signal"], row["ma_50"], row["ma_200"],
                float(row["rel_volume"]) if not pd.isna(row["rel_volume"]) else None,
                bool(row["is_uptrend"]) if not pd.isna(row["is_uptrend"]) else None
            ))
            inserted += 1
            if inserted % 1000 == 0:
                print(f"✅ Inserted {inserted} rows so far...")
        except Exception as e:
            print(f"⚠️ Skipped row: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Inserted {inserted} rows into quant_features_intraday")

# === Main ===
def engineer_features(symbol):
    start_time = time.time()
    print(f"🔍 Engineering features for: {symbol}")

    conn = connect()
    query = """
        SELECT datetime, open, high, low, close, volume
        FROM quant_candles_intraday
        WHERE ticker = %s
        ORDER BY datetime ASC
    """
    df = pd.read_sql_query(query, conn, params=(symbol,))
    conn.close()

    print(f"📊 Retrieved {len(df)} rows")
    if df.empty:
        print("⚠️ No data found.")
        return

    df = compute_indicators(df)
    store_features(symbol, df)

    elapsed = time.time() - start_time
    print(f"⏱️ Done in {elapsed:.2f} seconds")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        engineer_features(ticker)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_engineer_features.py GRRR")
