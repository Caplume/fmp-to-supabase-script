import psycopg2
import pandas as pd
import os
from datetime import datetime

# === Config ===
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")

# === Connect to Supabase ===
def connect():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return psycopg2.connect(conn_string)

# === Load Features and Candle Data ===
def load_data(ticker):
    conn = connect()
    query = f"""
        SELECT f.datetime, f.rsi, f.vwap, f.macd, f.macd_signal, f.ma_50, f.ma_200, c.close
        FROM quant_features_intraday f
        JOIN quant_candles_intraday c
        ON f.ticker = c.ticker AND f.datetime = c.datetime
        WHERE f.ticker = %s
        ORDER BY f.datetime ASC;
    """
    df = pd.read_sql_query(query, conn, params=(ticker,))
    conn.close()
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df

# === Add Forward Returns ===
def add_forward_returns(df):
    df = df.copy()
    df["future_close_5"] = df["close"].shift(-5)
    df["future_close_10"] = df["close"].shift(-10)
    df["future_close_15"] = df["close"].shift(-15)

    df["return_5min"] = (df["future_close_5"] - df["close"]) / df["close"] * 100
    df["return_10min"] = (df["future_close_10"] - df["close"]) / df["close"] * 100
    df["return_15min"] = (df["future_close_15"] - df["close"]) / df["close"] * 100

    return df.drop(columns=["future_close_5", "future_close_10", "future_close_15"])

# === Save Labeled Data ===
def save_labeled_data(ticker, df):
    conn = connect()
    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        if pd.isnull(row["return_5min"]):
            continue

        cur.execute("""
            INSERT INTO quant_pattern_labeled_intraday (
                ticker, datetime, rsi, vwap, macd, macd_signal, ma_50, ma_200,
                close, return_5min, return_10min, return_15min
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, datetime) DO NOTHING;
        """, (
            ticker,
            row["datetime"],
            row["rsi"], row["vwap"], row["macd"], row["macd_signal"],
            row["ma_50"], row["ma_200"],
            row["close"],
            row["return_5min"], row["return_10min"], row["return_15min"]
        ))
        inserted += 1

    conn.commit()
    conn.close()
    print(f"✅ Labeled and inserted {inserted} rows for {ticker}.")

# === Main ===
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        print(f"🔍 Processing labeled returns for: {ticker}")
        df = load_data(ticker)
        df_labeled = add_forward_returns(df)
        save_labeled_data(ticker, df_labeled)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_label_forward_returns.py GRRR")
