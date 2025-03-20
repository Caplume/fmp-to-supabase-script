import psycopg2
import json
from datetime import datetime, timezone
import os
import sys
import requests

# ✅ Claude API Configuration (Using direct HTTP)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ✅ PostgreSQL Connection Credentials (Direct Connection)
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")  # Set via environment variables
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")  # Direct connection
DB_PORT = os.environ.get("DB_PORT", "5432")  # Standard PostgreSQL port

# ✅ Function to call Claude API
def call_claude_api(prompt, system_prompt):
    """Call Claude API directly using HTTP requests."""
    if not ANTHROPIC_API_KEY:
        print("⚠️ No Anthropic API key provided")
        return None
    
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "content-type": "application/json",
        "anthropic-version": "2023-06-01"
    }
    
    data = {
        "model": "claude-3-7-sonnet-20250219",
        "max_tokens": 4000,
        "temperature": 0.3,
        "system": system_prompt,
        "messages": [
            {
                "role": "user", 
                "content": prompt
            }
        ]
    }
    
    try:
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=data
        )
        
        if response.status_code == 200:
            return response.json()['content'][0]['text']
        else:
            print(f"❌ API request failed: {response.status_code} - {response.text}")
            return None
    
    except Exception as e:
        print(f"❌ Error calling Claude API: {e}")
        return None

# ✅ Fetch news analysis from Supabase
def fetch_news_analysis(symbol):
    """Retrieve news analysis for a given stock symbol."""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting to fetch news analysis...")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()

        cur.execute("""
            SELECT metric, bull_case, base_case, bear_case, importance, confidence
            FROM ai_forecasts
            WHERE symbol = %s
        """, (symbol,))
        
        results = cur.fetchall()
        conn.close()
        
        if not results:
            print(f"⚠️ No news analysis found for {symbol}")
            return []
        
        analysis = []
        for row in results:
            analysis.append({
                "metric": row[0],
                "bull_case": row[1],
                "base_case": row[2],
                "bear_case": row[3],
                "importance": row[4],
                "confidence": row[5],
                "source": "news"
            })
        
        return analysis

    except Exception as e:
        print(f"❌ Database Error fetching news analysis: {e}")
        return []

# ✅ Fetch SEC filings analysis from Supabase
def fetch_sec_analysis(symbol):
    """Retrieve SEC filings analysis for a given stock symbol."""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting to fetch SEC analysis...")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()

        cur.execute("""
            SELECT metric, bull_case, base_case, bear_case, importance, confidence
            FROM ai_news_forecasts
            WHERE symbol = %s
        """, (symbol,))
        
        results = cur.fetchall()
        conn.close()
        
        if not results:
            print(f"⚠️ No SEC analysis found for {symbol}")
            return []
        
        analysis = []
        for row in results:
            analysis.append({
                "metric": row[0],
                "bull_case": row[1],
                "base_case": row[2],
                "bear_case": row[3],
                "importance": row[4],
                "confidence": row[5],
                "source": "sec_filings"
            })
        
        return analysis

    except Exception as e:
        print(f"❌ Database Error fetching SEC analysis: {e}")
        return []

# ✅ Fetch historical financial data
def fetch_historical_data(symbol):
    """Retrieve historical financial data for a given stock symbol."""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting to fetch historical data...")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()

        # Call the get_all_financial_data function
        cur.execute("SELECT get_all_financial_data(%s);", (symbol,))
        result = cur.fetchone()[0]
        conn.close()
        
        if not result:
            print(f"⚠️ No historical data found for {symbol}")
            return {}
        
        return result

    except Exception as e:
        print(f"❌ Database Error fetching historical data: {e}")
        return {}

# ✅ Generate comprehensive forecast with Claude
def generate_comprehensive_forecast(symbol, news_analysis, sec_analysis, historical_data):
    """Generate comprehensive forecast using Claude API."""
    system_prompt = """You are a senior financial analyst specializing in DCF models and forward-looking financial projections.
    
    Your task is to create detailed 5-year forward projections for key financial metrics based on historical data and qualitative analyses of news articles and SEC filings.
    
    For each metric and each year (1-5), provide:
    1. A specific numerical value (percentage) for bull case, base case, and bear case
    2. A comprehensive rationale that synthesizes insights from news, SEC filings, and historical trends
    
    Format your response as structured JSON with the following schema:
    
    {
      "forecasts": [
        {
          "metric": "Revenue Growth (%)",
          "year": 1,
          "bull_case": {"value": 25.4, "rationale": "Detailed justification..."},
          "base_case": {"value": 18.2, "rationale": "Detailed justification..."},
          "bear_case": {"value": 12.5, "rationale": "Detailed justification..."}
        },
        // Repeat for each metric and each year (1-5)
      ]
    }
    
    Focus on these key metrics:
    1. Revenue Growth (%)
    2. Gross Profit Margin (%)
    3. EBITDA Margin (%)
    4. FCF (% of Revenue)
    5. CapEx (% of Revenue)
    
    Make sure each numerical projection is reasonable based on the company's history, industry trends, and the provided analyses. Justify each projection with a concise but comprehensive rationale.
    """
    
    # Prepare the data
    news_analysis_json = json.dumps(news_analysis, indent=2)
    sec_analysis_json = json.dumps(sec_analysis, indent=2)
    historical_data_json = json.dumps(historical_data, indent=2)
    
    # Create the prompt
    prompt = f"""Generate a comprehensive 5-year financial forecast for {symbol}.

Based on the following inputs:

1. News and Press Release Analysis:
{news_analysis_json}

2. SEC Filings Analysis:
{sec_analysis_json}

3. Historical Financial Data:
{historical_data_json}

Create detailed projections for each key metric (Revenue Growth, Gross Profit Margin, EBITDA Margin, FCF, and CapEx) for each of the next 5 years.

For each metric and year, provide specific numerical values (percentages) for bull, base, and bear cases, along with detailed rationales that synthesize insights from all available information.

Return your analysis in the JSON format specified in the system instructions.
"""
    
    # Call Claude API
    response_text = call_claude_api(prompt, system_prompt)
    
    if not response_text:
        print("❌ Failed to generate forecast with Claude API")
        return None
    
    print("\n--- Claude's Final Forecast ---")
    print(response_text[:500] + "..." if len(response_text) > 500 else response_text)
    print("------------------------\n")
    
    # Parse the response as JSON
    try:
        # Try to find the JSON object in the response
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            forecast_data = json.loads(json_str)
            return forecast_data
        else:
            print("❌ Failed to extract JSON from Claude's response")
            return None
    
    except Exception as e:
        print(f"❌ Error parsing JSON from Claude's response: {e}")
        return None

# ✅ Store comprehensive forecast in Supabase
def store_comprehensive_forecast(symbol, forecast_data):
    """Store comprehensive forecast in Supabase."""
    if not forecast_data or 'forecasts' not in forecast_data:
        print("❌ Invalid forecast data format")
        return False
    
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting to store comprehensive forecast...")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()
        
        # Clear existing forecasts for this symbol
        cur.execute("DELETE FROM final_forecasts WHERE symbol = %s", (symbol,))
        
        # Insert new forecasts
        for forecast in forecast_data['forecasts']:
            metric = forecast['metric']
            year = forecast['year']
            
            # Bull case
            cur.execute("""
                INSERT INTO final_forecasts (symbol, metric, year, case_type, value, rationale)
                VALUES (%s, %s, %s, 'bull', %s, %s)
            """, (
                symbol, 
                metric, 
                year, 
                forecast['bull_case']['value'], 
                forecast['bull_case']['rationale']
            ))
            
            # Base case
            cur.execute("""
                INSERT INTO final_forecasts (symbol, metric, year, case_type, value, rationale)
                VALUES (%s, %s, %s, 'base', %s, %s)
            """, (
                symbol, 
                metric, 
                year, 
                forecast['base_case']['value'], 
                forecast['base_case']['rationale']
            ))
            
            # Bear case
            cur.execute("""
                INSERT INTO final_forecasts (symbol, metric, year, case_type, value, rationale)
                VALUES (%s, %s, %s, 'bear', %s, %s)
            """, (
                symbol, 
                metric, 
                year, 
                forecast['bear_case']['value'], 
                forecast['bear_case']['rationale']
            ))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Comprehensive forecast stored for {symbol}")
        return True
    
    except Exception as e:
        print(f"❌ Database Error storing forecast: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return False

# ✅ Main function
def generate_and_store_comprehensive_forecast(symbol):
    """Main function to generate and store comprehensive forecast."""
    print(f"🔍 Generating comprehensive forecast for {symbol}...")
    
    # Fetch all required data
    news_analysis = fetch_news_analysis(symbol)
    sec_analysis = fetch_sec_analysis(symbol)
    historical_data = fetch_historical_data(symbol)
    
    # Check if we have all the data we need
    if not news_analysis:
        print(f"⚠️ Missing news analysis for {symbol}")
    if not sec_analysis:
        print(f"⚠️ Missing SEC analysis for {symbol}")
    if not historical_data:
        print(f"⚠️ Missing historical data for {symbol}")
    
    if not news_analysis and not sec_analysis:
        print(f"❌ Cannot generate forecast without any analysis data")
        return False
    
    # Generate forecast
    forecast_data = generate_comprehensive_forecast(symbol, news_analysis, sec_analysis, historical_data)
    
    if not forecast_data:
        print(f"❌ Failed to generate comprehensive forecast for {symbol}")
        return False
    
    # Store forecast
    success = store_comprehensive_forecast(symbol, forecast_data)
    
    if success:
        print(f"🚀 Comprehensive forecast for {symbol} completed and stored!")
        return True
    else:
        print(f"❌ Failed to store comprehensive forecast for {symbol}")
        return False

# ✅ Execute when run as script
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "AAPL"
        print(f"⚠️ No ticker provided, using default: {symbol}")
    
    generate_and_store_comprehensive_forecast(symbol)