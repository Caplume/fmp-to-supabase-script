import requests
import psycopg2
from datetime import datetime, timezone
import os
import sys

# ✅ PostgreSQL Connection Credentials (Direct Connection)
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "3elJBVtbBkZ1YEdr")
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")  # Direct connection
DB_PORT = os.environ.get("DB_PORT", "5432")  # Standard PostgreSQL port

# ✅ FMP API Endpoint for Press Releases
FMP_API_KEY = os.environ.get("FMP_API_KEY", "E1x0AcpDC2qVyv4ebf2W9Wjge9EemKGw")
FMP_URL_TEMPLATE = "https://financialmodelingprep.com/api/v3/press-releases/{symbol}?apikey={apikey}"

def fetch_press_releases(symbol):
    """Fetch press releases from FMP API."""
    url = FMP_URL_TEMPLATE.format(symbol=symbol, apikey=FMP_API_KEY)
    print(f"📡 Fetching press releases for: {symbol}...")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        press_releases = response.json()  # ✅ Directly handle as a list
        
        if not isinstance(press_releases, list):  # ✅ Validate response format
            print(f"⚠️ Unexpected response format: {press_releases}")
            return []

        if not press_releases:
            print(f"⚠️ No press releases found for {symbol}.")
            return []

        return press_releases[:60]  # ✅ Limit to latest 60 press releases

    except requests.exceptions.RequestException as e:
        print(f"❌ API Error: {e}")
        return []

def store_press_releases_in_supabase(symbol, press_releases):
    """Store fetched press releases into Supabase."""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting with: {conn_string}")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()

        for pr in press_releases:
            published_date = pr.get("date", None)
            title = pr.get("title", "No Title")
            summary = pr.get("text", "No Summary")

            if not published_date:  # ✅ Skip entries with missing date
                print(f"⚠️ Skipping invalid entry: {pr}")
                continue
            
            cur.execute("""
                INSERT INTO press_releases (symbol, title, date, full_text, last_updated)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (symbol, date) DO NOTHING;
            """, (symbol, title, published_date, summary, datetime.now(timezone.utc)))

        conn.commit()
        conn.close()
        print(f"✅ Successfully stored {len(press_releases)} press releases for {symbol}.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

def fetch_and_store_press_releases(symbol):
    """Main function to fetch and store press releases."""
    press_releases = fetch_press_releases(symbol)
    if press_releases:
        store_press_releases_in_supabase(symbol, press_releases)

# ✅ Run the script with a test symbol or command line argument
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "AAPL"
        print(f"⚠️ No ticker provided, using default: {symbol}")
    
    fetch_and_store_press_releases(symbol)