import requests
import time
import random
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os
import sys

# ✅ PostgreSQL Connection Credentials (Direct Connection)
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "3elJBVtbBkZ1YEdr")
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")  # Direct connection
DB_PORT = os.environ.get("DB_PORT", "5432")  # Standard PostgreSQL port

# ✅ SEC-Compliant Headers (Using your real name & email)
HEADERS = {
    "User-Agent": "Louis Kahn (music.kahn@gmail.com)",  # 🔹 SEC requires proper identification
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "www.sec.gov"
}

def fetch_latest_filing_urls(symbol):
    """Retrieve the latest 10-K and 10-Q URLs from income_statements"""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting with: {conn_string}")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()
        
        cur.execute("""
            SELECT "finalLink", "period", "date"
            FROM income_statements
            WHERE symbol = %s
            ORDER BY "date" DESC
            LIMIT 10;
        """, (symbol,))
        
        filings = cur.fetchall()
        conn.close()
        
        print(f"🔍 Debug: Retrieved {len(filings)} filings for {symbol}: {filings}") 
        
        latest_10k = next(((f[0], f[2]) for f in filings if f[1] == 'FY'), (None, None))
        latest_10q = next(((f[0], f[2]) for f in filings if f[1].startswith('Q')), (None, None))
        
        print(f"🔍 Debug: Latest 10-K: {latest_10k}")
        print(f"🔍 Debug: Latest 10-Q: {latest_10q}")

        return latest_10k, latest_10q

    except Exception as e:
        print(f"❌ Database Error: {e}")
        return None, None

def scrape_sec_filing(url):
    """Scrape the SEC filing page and return the extracted text."""
    if not url:
        return None

    for attempt in range(3):  # ✅ Retry mechanism (up to 3 attempts)
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                # ✅ Extract text while filtering unnecessary elements
                for script in soup(["script", "style"]):
                    script.extract()
                
                text = soup.get_text(separator=" ", strip=True)
                return text[:300000]  # ✅ Limit text size to avoid huge database entries
            
            elif response.status_code == 403:
                print(f"❌ Access Denied (403). Retrying attempt {attempt+1}...")
                time.sleep(random.uniform(2, 4))  # ✅ Wait before retrying
            
            else:
                print(f"⚠️ HTTP Error {response.status_code}: {url}")
                return None
        
        except Exception as e:
            print(f"⚠️ Scraping Error: {e}. Retrying ({attempt+1}/3)...")
            time.sleep(random.uniform(2, 4))
    
    return None  # Return None if all attempts fail

def store_filing_in_supabase(symbol, filing_type, url, filing_date, full_text):
    """Store the scraped SEC filing text in Supabase"""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO sec_filings (symbol, filing_type, filing_url, filing_date, full_text, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (filing_url) DO NOTHING;
        """, (symbol, filing_type, url, filing_date, full_text, datetime.now(timezone.utc)))
        
        conn.commit()
        conn.close()
        print(f"✅ Successfully stored {filing_type} for {symbol}.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

def fetch_and_store_sec_filings(symbol):
    """Main function to retrieve, scrape, and store SEC filings"""
    print(f"📡 Fetching SEC filings for: {symbol}...")

    (latest_10k_url, latest_10k_date), (latest_10q_url, latest_10q_date) = fetch_latest_filing_urls(symbol)

    # ✅ Process 10-K
    if latest_10k_url:
        print(f"📄 Scraping 10-K: {latest_10k_url}")
        full_text = scrape_sec_filing(latest_10k_url)
        if not full_text:
            full_text = "[No text extracted]"  # ✅ Avoid NULL in DB
        store_filing_in_supabase(symbol, "10-K", latest_10k_url, latest_10k_date, full_text)

    # ✅ Process 10-Q
    if latest_10q_url:
        print(f"📄 Scraping 10-Q: {latest_10q_url}")
        full_text = scrape_sec_filing(latest_10q_url)
        if not full_text:
            full_text = "[No text extracted]"  # ✅ Avoid NULL in DB
        store_filing_in_supabase(symbol, "10-Q", latest_10q_url, latest_10q_date, full_text)

# ✅ Run the script with a test symbol or command line argument
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "AAPL"
        print(f"⚠️ No ticker provided, using default: {symbol}")
    
    fetch_and_store_sec_filings(symbol)