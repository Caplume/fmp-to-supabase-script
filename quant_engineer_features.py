import psycopg2
import pandas as pd
import os
from datetime import datetime, timedelta

# === Config ===
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT", "5432")

# === Connect ===
def connect():
    conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return psycopg2.connect(conn_string)

# === Load Raw Candle Data ===
def load_intraday_data(ticker):
    conn = connect()
    query = """
        SELECT * FROM quant_candles_intraday
        WHERE ticker = %s
        ORDER BY datetime;
    """
    df = pd.read_sql_query(query, conn, params=(ticker,))
    conn.close()
    return df

# === Feature Engineering ===
def engineer_features(df):
    df = df.copy()
    df['close'] = df['close'].astype(float)
    df['rsi'] = df['close'].rolling(window=14).apply(lambda x: pd.Series(x).diff().pipe(lambda y: y[y > 0].sum() / abs(y[y < 0].sum()) if abs(y[y < 0].sum()) > 0 else 0)).apply(lambda rs: 100 - (100 / (1 + rs)))
    df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
    df['ma_50'] = df['close'].rolling(window=50).mean()
    df['ma_200'] = df['close'].rolling(window=200).mean()

    # MACD
    ema_12 = df['close'].ewm(span=12, adjust=False).mean()
    ema_26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = ema_12 - ema_26
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()

    # Volume Spike
    df['avg_vol_20'] = df['volume'].rolling(window=20).mean()
    df['rel_volume'] = df['volume'] / df['avg_vol_20']

    # Trend Filter
    df['is_uptrend'] = (df['ma_50'] > df['ma_200']).astype(int)

    return df

# === Store Features ===
def store_features(ticker, df):
    conn = connect()
    cur = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        if pd.isnull(row['rsi']) or pd.isnull(row['macd']) or pd.isnull(row['vwap']):
            continue

        try:
            cur.execute("""
                INSERT INTO quant_features_intraday (ticker, datetime, rsi, vwap, macd, macd_signal, ma_50, ma_200, rel_volume, is_uptrend)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime) DO NOTHING;
            """, (
                ticker,
                row['datetime'],
                row['rsi'],
                row['vwap'],
                row['macd'],
                row['macd_signal'],
                row['ma_50'],
                row['ma_200'],
                row['rel_volume'],
                row['is_uptrend']
            ))
            inserted += 1
        except Exception as e:
            print(f"⚠️ Skipped row: {e}")

    conn.commit()
    conn.close()
    print(f"✅ Inserted {inserted} engineered rows for {ticker}.")

# === Main ===
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        print(f"🔍 Engineering features for: {ticker}\n")
        df = load_intraday_data(ticker)
        df = engineer_features(df)
        store_features(ticker, df)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_engineer_features.py GRRR")
