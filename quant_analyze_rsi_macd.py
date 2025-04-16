import psycopg2
import pandas as pd
import os

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

# === Load Labeled Data ===
def load_labeled_data(ticker):
    conn = connect()
    query = """
        SELECT datetime, rsi, macd, macd_signal, return_5min, return_10min, return_15min
        FROM quant_pattern_labeled_intraday
        WHERE ticker = %s AND rsi IS NOT NULL AND macd IS NOT NULL AND macd_signal IS NOT NULL
        ORDER BY datetime;
    """
    df = pd.read_sql_query(query, conn, params=(ticker,))
    conn.close()
    return df

# === Analyze RSI + MACD Histogram ===
def analyze_rsi_macd(df):
    df = df.copy()

    # RSI Buckets
    df["rsi_bucket"] = pd.cut(df["rsi"], bins=[0, 20, 30, 40, 50, 60, 70, 80, 100])

    # MACD Histogram = MACD - MACD Signal
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    df["macd_bucket"] = pd.cut(df["macd_hist"], bins=[-5, -0.1, -0.01, 0.0, 0.01, 0.1, 5])

    grouped = df.groupby(["rsi_bucket", "macd_bucket"]).agg(
        count=("return_10min", "count"),
        avg_return_5min=("return_5min", "mean"),
        avg_return_10min=("return_10min", "mean"),
        avg_return_15min=("return_15min", "mean")
    ).reset_index()
    return grouped

# === Main ===
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        ticker = sys.argv[1].upper()
        print(f"🔍 Analyzing RSI + MACD histogram buckets for: {ticker}\n")
        df = load_labeled_data(ticker)
        result = analyze_rsi_macd(df)
        print(result)
    else:
        print("❌ Please provide a ticker symbol. Example: python3 quant_analyze_rsi_macd.py GRRR")
