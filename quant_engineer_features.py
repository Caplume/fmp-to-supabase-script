import psycopg2
import pandas as pd
import ta
import os
from datetime import datetime

# === Supabase Connection ===
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")

# === Ticker to Process ===
TICKER = "GRRR"

# === Connect to Supabase DB ===
def connect():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return psycopg2.connect(conn_string)

# === Load raw candles ===
def load_candles():
    conn = connect()
    query = f"""
        SELECT datetime, open, high, low, close, volume
        FROM quant_candles_intraday
        WHERE ticker = %s
        ORDER BY datetime ASC;
    """
    df = pd.read_sql_query(query, conn, params=(TICKER,))
    conn.close()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    return df

# === Calculate indicators ===
def calculate_features(df):
    df = df.copy()
    df["rsi"] = ta.momentum.RSIIndicator(close=df["close"], window=14).rsi()
    df["vwap"] = (df["volume"] * (df["high"] + df["low"] + df["close"]) / 3).cumsum() / df["volume"].cumsum()
    macd = ta.trend.MACD(close=df["close"])
    df["macd"] = macd.macd()
    df["macd_signal"] = macd.macd_signal()
    df["ma_50"] = df["close"].rolling(window=50).mean()
    df["ma_200"] = df["close"].rolling(window=200).mean()
    return df

# === Upload to Supabase ===
def upload_features(df):
    conn = connect()
    cur = conn.cursor()
    insert_count = 0

    for idx, row in df.iterrows():
        if pd.isnull(row.rsi):
            continue  # Skip rows with incomplete indicators

        cur.execute("""
            INSERT INTO quant_features_intraday (
                ticker, datetime, rsi, vwap, macd, macd_signal, ma_50, ma_200, candle_ref
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, datetime) DO NOTHING;
        """, (
            TICKER,
            idx,
            row.rsi,
            row.vwap,
            row.macd,
            row.macd_signal,
            row.ma_50,
            row.ma_200,
            idx  # candle_ref
        ))
        insert_count += 1

    conn.commit()
    conn.close()
    print(f"✅ Inserted {insert_count} feature rows for {TICKER}.")

# === Run all ===
if __name__ == "__main__":
    df_raw = load_candles()
    df_feat = calculate_features(df_raw)
    upload_features(df_feat)
