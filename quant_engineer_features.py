import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
import os
import sys

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

def compute_features(df):
    df = df.sort_values("datetime")
    
    # Relative Volume: current volume / rolling 50-period average
    df["rel_volume"] = df["volume"] / df["volume"].rolling(window=50).mean()

    # RSI Calculation
    delta = df["close"].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14).mean()
    avg_loss = pd.Series(loss).rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # VWAP
    df["cum_vol_price"] = (df["close"] * df["volume"]).cumsum()
    df["cum_volume"] = df["volume"].cumsum()
    df["vwap"] = df["cum_vol_price"] / df["cum_volume"]

    # MACD
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()

    # Moving Averages
    df["ma_50"] = df["close"].rolling(window=50).mean()
    df["ma_200"] = df["close"].rolling(window=200).mean()

    # Trend Flag
    df["is_uptrend"] = df["ma_50"] > df["ma_200"]

    # Clean up
    df = df.drop(columns=["cum_vol_price", "cum_volume"])
    df = df.dropna()

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
                symbol, row["datetime"], row["rsi"], row["vwap"],
                row["macd"], row["macd_signal"], row["ma_50"], row["ma_200"],
                row["rel_volume"], bool(row["is_uptrend"])  # ✅ Cast to boolean
            ))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Skipped row: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Stored {inserted} feature rows for {symbol}")

# === Main ===
def engineer(symbol):
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

    if df.empty:
        print("❌ No data found.")
        return

    df = compute_features(df)
    store_features(symbol, df)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        engineer(ticker)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_engineer_features.py GRRR")
