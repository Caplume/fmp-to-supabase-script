import requests
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import os
import sys

# Get credentials from environment variables (with fallbacks for local testing)
# ✅ PostgreSQL Connection Credentials (Supabase)
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "3elJBVtbBkZ1YEdr")
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")
DB_PORT = os.environ.get("DB_PORT", "5432")

# ✅ FMP API Key
FMP_API_KEY = os.environ.get("FMP_API_KEY", "E1x0AcpDC2qVyv4ebf2W9Wjge9EemKGw")

# ✅ Function: Fetch news articles from FMP API
def fetch_news_articles(symbol):
    """
    Fetches news articles for a given stock symbol from Financial Modeling Prep API.
    Filters out YouTube links and articles older than 1 year.
    Stops fetching if 10 valid articles are found.
    """
    today = datetime.today().strftime('%Y-%m-%d')
    one_year_ago = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')

    url = f"https://financialmodelingprep.com/stable/news/stock?symbols={symbol}&from={one_year_ago}&to={today}&limit=40&apikey={FMP_API_KEY}"
    
    print(f"📡 Fetching news for: {symbol}...")

    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ API request failed with status code {response.status_code}")
        return []

    news_data = response.json()

    articles = []
    for article in news_data:
        # ✅ Extract fields
        url = article.get("url", "")
        if "youtube.com" in url or "youtu.be" in url:
            print(f"⚠️ Skipping YouTube article: {url}")
            continue  # Skip YouTube videos

        articles.append({
            "symbol": symbol,
            "published_date": article.get("publishedDate"),
            "publisher": article.get("publisher", "Unknown"),
            "title": article.get("title"),
            "image": article.get("image"),
            "site": article.get("site"),
            "text_snippet": article.get("text"),
            "url": url,
            "full_text": scrape_article_text(url)
        })

        # ✅ Stop fetching more if we already have 10 valid articles
        if len(articles) >= 10:
            break

    return articles

# ✅ Function: Scrape Article Text
def scrape_article_text(url):
    """
    Scrapes the full article text from a given URL.
    """
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=5)

        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, "html.parser")
        paragraphs = soup.find_all("p")
        full_text = " ".join([p.get_text() for p in paragraphs])

        return full_text.strip() if full_text else None

    except Exception as e:
        print(f"⚠️ Scraping failed for {url}: {e}")
        return None

# ✅ Function: Store Articles in Supabase
def store_articles_in_supabase(articles):
    """
    Inserts scraped articles into the Supabase database.
    """
    if not articles:
        print("⚠️ No articles to store.")
        return 0

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()

        for article in articles:
            cur.execute("""
                INSERT INTO news_articles (symbol, published_date, publisher, title, image, site, text_snippet, url, full_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (
                article["symbol"], article["published_date"], article["publisher"],
                article["title"], article["image"], article["site"],
                article["text_snippet"], article["url"], article["full_text"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ Successfully stored {len(articles)} articles in Supabase.")
        return len(articles)

    except Exception as e:
        print(f"❌ Database Error: {e}")
        return 0

# ✅ Main Function
def fetch_and_store_news(symbol):
    articles = fetch_news_articles(symbol)
    stored_count = store_articles_in_supabase(articles)
    print(f"📥 {stored_count} articles stored.")

# ✅ Test with command line argument or default to AAPL
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "AAPL"
        print(f"⚠️ No ticker provided, using default: {symbol}")
    
    fetch_and_store_news(symbol)