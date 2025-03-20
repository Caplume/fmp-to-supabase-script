from flask import Flask, request, jsonify
import subprocess
import os
import sys

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy"}), 200

@app.route('/run-script', methods=['POST'])
def run_script():
    """Run a specific script with a ticker symbol."""
    data = request.json
    
    if not data or 'symbol' not in data:
        return jsonify({"error": "Missing required parameter: symbol"}), 400
    
    symbol = data['symbol']
    
    # Check if a specific script was requested
    script_name = data.get('script', 'scrape_articles_supabase_render.py')
    
    # Validate script name for security
    allowed_scripts = [
        "scrape_articles_supabase_render.py",
        "scrape_sec_filings_render.py",
        "fetch_press_releases_render.py",
        "analyze_news_sentiment_render.py",
        "analyze_sec_filings_render.py",
        "generate_comprehensive_forecast_render.py"
    ]
    
    if script_name not in allowed_scripts:
        return jsonify({
            "error": f"Invalid script name. Allowed scripts: {', '.join(allowed_scripts)}"
        }), 400
    
    try:
        # Get the full path to the script
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        
        # Run the script with the symbol as a command-line argument
        command = [sys.executable, script_path, symbol]
        process = subprocess.run(
            command,
            capture_output=True,
            text=True
        )
        
        return jsonify({
            "symbol": symbol,
            "script": script_name,
            "status": "success" if process.returncode == 0 else "error",
            "output": process.stdout,
            "error": process.stderr
        })
        
    except Exception as e:
        return jsonify({
            "symbol": symbol,
            "script": script_name,
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/run-pipeline', methods=['POST'])
def run_pipeline():
    """Run the entire pipeline of scripts for a given ticker symbol."""
    data = request.json
    
    if not data or 'symbol' not in data:
        return jsonify({"error": "Missing required parameter: symbol"}), 400
    
    symbol = data['symbol']
    
    # Define the sequence of scripts to run
    scripts = [
        "scrape_articles_supabase_render.py",
        "scrape_sec_filings.py",
        "fetch_press_releases.py",
        "analyze_news_sentiment.py",
        "analyze_sec_filings.py"
    ]
    
    results = []
    for script_name in scripts:
        try:
            script_path = os.path.join(os.path.dirname(__file__), script_name)
            
            # Run each script
            command = [sys.executable, script_path, symbol]
            process = subprocess.run(
                command,
                capture_output=True,
                text=True
            )
            
            results.append({
                "script": script_name,
                "status": "success" if process.returncode == 0 else "error",
                "output": process.stdout,
                "error": process.stderr
            })
            
        except Exception as e:
            results.append({
                "script": script_name,
                "status": "error",
                "error": str(e)
            })
    
    return jsonify({
        "symbol": symbol,
        "status": "completed",
        "results": results
    })

@app.route('/', methods=['GET'])
def index():
    """Home page with instructions."""
    html = '<html>'
    html += '<head><title>Financial Data Scripts API</title>'
    html += '<style>body{font-family:Arial,sans-serif;margin:40px;line-height:1.6;}'
    html += 'code{background:#f4f4f4;padding:2px 5px;}'
    html += 'pre{background:#f4f4f4;padding:10px;border-radius:5px;}</style></head>'
    html += '<body><h1>Financial Data Scripts API</h1>'
    html += '<p>Use this API to run financial data collection and analysis scripts.</p>'
    html += '<h2>Available Endpoints:</h2><ul>'
    html += '<li><code>GET /health</code> - Check if the service is running</li>'
    html += '<li><code>POST /run-script</code> - Run a specific script with a ticker symbol</li>'
    html += '<li><code>POST /run-pipeline</code> - Run the entire pipeline of scripts for a ticker</li>'
    html += '</ul>'
    
    html += '<h2>Example Usage (Single Script):</h2>'
    html += '<pre>POST /run-script\nContent-Type: application/json\n\n{'
    html += '"symbol": "AAPL", "script": "scrape_articles_supabase_render.py"}</pre>'
    
    html += '<h2>Example Usage (Full Pipeline):</h2>'
    html += '<pre>POST /run-pipeline\nContent-Type: application/json\n\n{'
    html += '"symbol": "AAPL"}</pre>'
    
    html += '<h2>Available Scripts:</h2><ul>'
    html += '<li>scrape_articles_supabase_render.py - Fetch news articles</li>'
    html += '<li>scrape_sec_filings.py - Fetch SEC filings</li>'
    html += '<li>fetch_press_releases.py - Fetch press releases</li>'
    html += '<li>analyze_news_sentiment.py - Analyze news sentiment</li>'
    html += '<li>analyze_sec_filings.py - Analyze SEC filings</li>'
    html += '</ul></body></html>'
    
    return html

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
