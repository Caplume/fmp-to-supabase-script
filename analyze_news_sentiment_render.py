import psycopg2
import json
from datetime import datetime, timezone
import os
import sys

try:
    import anthropic
except ImportError:
    print("Warning: anthropic module not available")
    anthropic = None

# ✅ Claude API Configuration (Using Official SDK)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# ✅ PostgreSQL Connection Credentials (Direct Connection)
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")  # Set via environment variables
DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")  # Direct connection
DB_PORT = os.environ.get("DB_PORT", "5432")  # Standard PostgreSQL port

# ✅ Fetch latest news & press releases from Supabase
def fetch_news_and_press(symbol):
    """Retrieve latest news articles and press releases for a given stock symbol."""
    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        print(f"Connecting with: {conn_string.replace(DB_PASSWORD, '********')}")
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()

        # ✅ Fetch news articles (use text_snippet if full_text is missing)
        cur.execute("""
            SELECT title, COALESCE(full_text, text_snippet) AS content
            FROM news_articles 
            WHERE symbol = %s 
            ORDER BY published_date DESC 
            LIMIT 10;
        """, (symbol,))
        news_articles = cur.fetchall()

        # ✅ Fetch press releases
        cur.execute("""
            SELECT title, full_text 
            FROM press_releases 
            WHERE symbol = %s 
            ORDER BY date DESC 
            LIMIT 10;
        """, (symbol,))
        press_releases = cur.fetchall()

        conn.close()

        return news_articles, press_releases

    except Exception as e:
        print(f"❌ Database Error: {e}")
        return [], []

# ✅ Truncate text to prevent exceeding API limits
def truncate_text(text, max_chars=500):
    """Ensure text is within token limits for API."""
    if text and len(text) > max_chars:
        return text[:max_chars] + "..."
    return text

# ✅ Create standard metrics list
def create_default_metrics():
    """Create a list of default metric entries."""
    metrics = []
    
    valid_metrics = [
        "Revenue Growth (%)",
        "Gross Profit Margin (%)",
        "EBITDA Margin (%)",
        "FCF (% of Revenue)",
        "CapEx (% of Revenue)"
    ]
    
    for metric in valid_metrics:
        metrics.append({
            "metric": metric,
            "bull_case": {"value": None, "rationale": "Based on positive news sentiment"},
            "base_case": {"value": None, "rationale": "Based on neutral assessment"},
            "bear_case": {"value": None, "rationale": "Based on negative news sentiment"},
            "importance": 3,
            "confidence": "Medium"
        })
    
    return metrics

# ✅ Send request to Claude API using Official SDK
def analyze_news_sentiment(symbol):
    """Send news and press releases to Claude for sentiment analysis."""
    news_articles, press_releases = fetch_news_and_press(symbol)

    if not news_articles and not press_releases:
        print(f"⚠️ No news or press releases available for {symbol}. Skipping analysis.")
        return create_default_metrics()

    # ✅ Format text for Claude API (truncate to avoid limits)
    combined_text = "\n\n".join([
        f"Title: {item[0]}\nText: {truncate_text(item[1])}" for item in news_articles + press_releases
    ])

    system_prompt = """You are a senior financial analyst. Analyze news articles and press releases to assess their impact on key financial assumptions.
    
    Please format your analysis as follows for each financial metric:
    
    ## [Metric Name]
    
    Bull Case: [Brief rationale for positive outlook]
    
    Base Case: [Brief rationale for neutral outlook]
    
    Bear Case: [Brief rationale for negative outlook]
    
    Importance: [Low/Medium/High]
    
    Confidence: [Low/Medium/High]
    """

    user_prompt = f"""Analyze the following news and press releases for {symbol}:

{combined_text}

Provide detailed analysis for these specific financial metrics:
1. Revenue Growth (%)
2. Gross Profit Margin (%)
3. EBITDA Margin (%)
4. FCF (% of Revenue)
5. CapEx (% of Revenue)

For each metric, explain how the news affects bull case, base case, and bear case scenarios."""

    if anthropic is None:
        print("⚠️ Anthropic module not available - using simulated analysis")
        text_content = f"""## Revenue Growth (%)

Bull Case: Strong product demand for {symbol} suggests continued revenue growth.

Base Case: Market conditions indicate stable growth potential.

Bear Case: Competitive pressures may impact revenue expectations.

Importance: High

Confidence: Medium

## Gross Profit Margin (%)

Bull Case: Product mix shifts toward higher-margin offerings.

Base Case: Margins likely to remain consistent with historical trends.

Bear Case: Input cost increases could pressure margins.

Importance: Medium

Confidence: Medium"""
    else:
        # Initialize the client carefully
        try:
            if hasattr(anthropic, 'Anthropic'):
                client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
            else:
                client = anthropic.Client(api_key=ANTHROPIC_API_KEY)
                
            # Try the newer messages API first
            try:
                if hasattr(client, 'messages') and hasattr(client.messages, 'create'):
                    print("Using newer Anthropic messages API")
                    message = client.messages.create(
                        model="claude-3-7-sonnet-20250219",
                        max_tokens=1500,
                        temperature=0.3,
                        system=system_prompt,
                        messages=[
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": user_prompt
                                    }
                                ]
                            }
                        ]
                    )
                    
                    # Extract text content from the structured response
                    text_content = ""
                    for block in message.content:
                        if hasattr(block, 'text'):
                            text_content += block.text
                        elif isinstance(block, dict) and 'text' in block:
                            text_content += block['text']
                
                # Fall back to older API if needed
                else:
                    print("Using legacy Anthropic completion API")
                    full_prompt = f"\n\nHuman: {user_prompt}\n\nAssistant:"
                    if hasattr(client, 'completion'):
                        response = client.completion(
                            prompt=full_prompt,
                            model="claude-2.0",
                            max_tokens_to_sample=1500,
                            temperature=0.3
                        )
                        text_content = response.completion
                    else:
                        print("Neither messages.create nor completion method found")
                        text_content = "API method not found"
                        
            except Exception as e:
                print(f"❌ Error calling Anthropic API method: {e}")
                print("Falling back to simulated analysis")
                return create_default_metrics()
                
        except Exception as e:
            print(f"❌ Error initializing Anthropic client: {e}")
            print("Falling back to simulated analysis")
            return create_default_metrics()
    
    print("\n--- Claude's Response ---")
    print(text_content[:500] + "..." if len(text_content) > 500 else text_content)
    print("------------------------\n")
    
    # Initialize metrics with defaults
    metrics = create_default_metrics()
    
    try:
        # Split by sections (each metric will have its own section)
        sections = []
        if "##" in text_content:
            # Split by markdown headers
            raw_sections = text_content.split("##")
            for section in raw_sections[1:]:  # Skip the first which is just intro text
                section = section.strip()
                # Get the title from the first line
                lines = section.split("\n")
                title = lines[0].strip()
                content = "\n".join(lines[1:]).strip()
                sections.append({"title": title, "content": content})
        else:
            # Try to find metrics names directly
            valid_metrics = [
                "Revenue Growth",
                "Gross Profit Margin",
                "EBITDA Margin",
                "FCF",
                "CapEx"
            ]
            
            for metric in valid_metrics:
                pattern = f"{metric}"
                if pattern in text_content:
                    # Find the section for this metric
                    start_idx = text_content.find(pattern)
                    # Find the next metric or the end
                    end_idx = len(text_content)
                    for next_metric in valid_metrics:
                        if next_metric != metric and next_metric in text_content[start_idx:]:
                            next_idx = text_content[start_idx:].find(next_metric) + start_idx
                            if next_idx < end_idx:
                                end_idx = next_idx
                    
                    section_content = text_content[start_idx:end_idx].strip()
                    sections.append({"title": metric, "content": section_content})
        
        # Process each section to update the corresponding metric
        for section in sections:
            title = section["title"]
            content = section["content"]
            
            # Map the section title to a valid metric
            metric_key = None
            if "Revenue Growth" in title:
                metric_key = "Revenue Growth (%)"
            elif "Gross Profit Margin" in title or "Gross Margin" in title:
                metric_key = "Gross Profit Margin (%)"
            elif "EBITDA Margin" in title:
                metric_key = "EBITDA Margin (%)"
            elif "FCF" in title or "Free Cash Flow" in title:
                metric_key = "FCF (% of Revenue)"
            elif "CapEx" in title or "Capital Expenditure" in title:
                metric_key = "CapEx (% of Revenue)"
            
            if not metric_key:
                continue
            
            # Find the matching metric in our list
            target_metric = None
            for metric in metrics:
                if metric["metric"] == metric_key:
                    target_metric = metric
                    break
            
            if not target_metric:
                continue
            
            # Extract bull case
            bull_case = "Based on positive news sentiment"
            bull_patterns = ["Bull Case:", "Bullish:", "Positive:", "Bull:"]
            for pattern in bull_patterns:
                if pattern in content:
                    start_idx = content.find(pattern) + len(pattern)
                    end_idx = content[start_idx:].find("\n\n")
                    if end_idx == -1:
                        for other_pattern in ["Base Case:", "Bear Case:", "Importance:", "Confidence:"]:
                            if other_pattern in content[start_idx:]:
                                other_idx = content[start_idx:].find(other_pattern)
                                if end_idx == -1 or other_idx < end_idx:
                                    end_idx = other_idx
                    
                    if end_idx == -1:
                        bull_text = content[start_idx:].strip()
                    else:
                        bull_text = content[start_idx:start_idx+end_idx].strip()
                    
                    if bull_text:
                        bull_case = bull_text
                    break
            
            target_metric["bull_case"]["rationale"] = bull_case
            
            # Extract base case
            base_case = "Based on neutral assessment"
            base_patterns = ["Base Case:", "Neutral:", "Base:"]
            for pattern in base_patterns:
                if pattern in content:
                    start_idx = content.find(pattern) + len(pattern)
                    end_idx = content[start_idx:].find("\n\n")
                    if end_idx == -1:
                        for other_pattern in ["Bear Case:", "Bull Case:", "Importance:", "Confidence:"]:
                            if other_pattern in content[start_idx:]:
                                other_idx = content[start_idx:].find(other_pattern)
                                if end_idx == -1 or other_idx < end_idx:
                                    end_idx = other_idx
                    
                    if end_idx == -1:
                        base_text = content[start_idx:].strip()
                    else:
                        base_text = content[start_idx:start_idx+end_idx].strip()
                    
                    if base_text:
                        base_case = base_text
                    break
            
            target_metric["base_case"]["rationale"] = base_case
            
            # Extract bear case
            bear_case = "Based on negative news sentiment"
            bear_patterns = ["Bear Case:", "Bearish:", "Negative:", "Bear:"]
            for pattern in bear_patterns:
                if pattern in content:
                    start_idx = content.find(pattern) + len(pattern)
                    end_idx = content[start_idx:].find("\n\n")
                    if end_idx == -1:
                        for other_pattern in ["Bull Case:", "Base Case:", "Importance:", "Confidence:"]:
                            if other_pattern in content[start_idx:]:
                                other_idx = content[start_idx:].find(other_pattern)
                                if end_idx == -1 or other_idx < end_idx:
                                    end_idx = other_idx
                    
                    if end_idx == -1:
                        bear_text = content[start_idx:].strip()
                    else:
                        bear_text = content[start_idx:start_idx+end_idx].strip()
                    
                    if bear_text:
                        bear_case = bear_text
                    break
            
            target_metric["bear_case"]["rationale"] = bear_case
            
            # Extract importance
            if "Importance:" in content:
                if "High" in content[content.find("Importance:"):content.find("Importance:")+30]:
                    target_metric["importance"] = 5
                elif "Low" in content[content.find("Importance:"):content.find("Importance:")+30]:
                    target_metric["importance"] = 1
                # Medium is already the default (3)
            
            # Extract confidence
            if "Confidence:" in content:
                if "High" in content[content.find("Confidence:"):content.find("Confidence:")+30]:
                    target_metric["confidence"] = "High"
                elif "Low" in content[content.find("Confidence:"):content.find("Confidence:")+30]:
                    target_metric["confidence"] = "Low"
                # Medium is already the default
    
    except Exception as e:
        print(f"Error parsing Claude response: {e}")
        import traceback
        traceback.print_exc()
        # Continue with default metrics
    
    return metrics

# ✅ Store AI response in Supabase
def store_ai_forecast(symbol, metrics):
    """Insert AI-generated forecasts into Supabase."""
    if not metrics:
        print(f"⚠️ No valid metrics received for {symbol}. Skipping storage.")
        return

    try:
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string)
        
        cur = conn.cursor()
        
        # Clear previous entries for this symbol to avoid duplicates
        cur.execute("""
            DELETE FROM ai_forecasts WHERE symbol = %s;
        """, (symbol,))
        
        # Insert each metric as a separate row
        for metric_data in metrics:
            cur.execute("""
                INSERT INTO ai_forecasts (
                    symbol, 
                    metric, 
                    bull_case, 
                    base_case, 
                    bear_case, 
                    importance, 
                    confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                symbol,
                metric_data["metric"],
                json.dumps(metric_data["bull_case"]),
                json.dumps(metric_data["base_case"]),
                json.dumps(metric_data["bear_case"]),
                metric_data["importance"],
                metric_data["confidence"]
            ))

        conn.commit()
        conn.close()
        print(f"✅ AI forecast metrics stored for {symbol}.")

    except Exception as e:
        print(f"❌ Database Error: {e}")
        if 'conn' in locals() and conn:
            conn.close()

# ✅ Main execution
if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        print(f"🔍 Using provided ticker symbol: {symbol}")
    else:
        symbol = "AAPL"
        print(f"⚠️ No ticker provided, using default: {symbol}")
    
    metrics = analyze_news_sentiment(symbol)
    if metrics:
        store_ai_forecast(symbol, metrics)
        print(f"🚀 AI sentiment analysis for {symbol} completed and stored!")
