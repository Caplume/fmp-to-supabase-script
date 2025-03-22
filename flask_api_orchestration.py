from flask import Flask, request, jsonify
import subprocess
import os
import sys
import threading
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Simple in-memory storage for job status
job_status = {}

def run_orchestration_in_background(job_id, symbol):
    """Run the orchestration script in a background thread"""
    try:
        job_status[job_id] = {"status": "running", "symbol": symbol}
        logger.info(f"Starting orchestration for {symbol}, job ID: {job_id}")
        
        script_path = os.path.join(os.path.dirname(__file__), "orchestration-script.py")
        process = subprocess.run(
            [sys.executable, script_path, symbol],
            capture_output=True,
            text=True
        )
        
        if process.returncode == 0:
            job_status[job_id] = {"status": "completed", "symbol": symbol}
            logger.info(f"Orchestration completed for {symbol}, job ID: {job_id}")
        else:
            job_status[job_id] = {
                "status": "failed", 
                "symbol": symbol, 
                "error": process.stderr
            }
            logger.error(f"Orchestration failed for {symbol}: {process.stderr}")
            
    except Exception as e:
        job_status[job_id] = {"status": "failed", "symbol": symbol, "error": str(e)}
        logger.error(f"Exception during orchestration: {str(e)}")

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({"status": "healthy"}), 200

@app.route('/run-pipeline', methods=['POST'])
def run_pipeline():
    """Run the orchestration script for a given ticker symbol."""
    data = request.json
    
    if not data or 'symbol' not in data:
        return jsonify({"error": "Missing required parameter: symbol"}), 400
    
    symbol = data['symbol']
    job_id = f"{symbol}-{len(job_status)}"
    
    # Start orchestration in background thread
    thread = threading.Thread(
        target=run_orchestration_in_background, 
        args=(job_id, symbol)
    )
    thread.daemon = True
    thread.start()
    
    return jsonify({
        "job_id": job_id,
        "symbol": symbol,
        "status": "started",
        "message": f"Pipeline started for {symbol}"
    })

@app.route('/job-status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Check the status of a job"""
    if job_id in job_status:
        return jsonify(job_status[job_id])
    else:
        return jsonify({"status": "not_found", "job_id": job_id}), 404

@app.route('/run-script', methods=['POST'])
def run_script():
    """Run a specific script with a ticker symbol (for backward compatibility)."""
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
        "generate_comprehensive_forecast_render.py",
        "orchestration-script.py"
    ]
    
    if script_name not in allowed_scripts:
        return jsonify({
            "error": f"Invalid script name. Allowed scripts: {', '.join(allowed_scripts)}"
        }), 400
    
    try:
        # Get the full path to the script
        script_path = os.path.join(os.path.dirname(__file__), script_name)
        
        # Run the script with the symbol as a command-line argument
        process = subprocess.run(
            [sys.executable, script_path, symbol],
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

@app.route('/get-forecast/<symbol>', methods=['GET'])
def get_forecast(symbol):
    """Retrieve forecast data for a specific symbol."""
    try:
        # This is a placeholder - you'll need to import and use psycopg2 here
        # For now, we'll return a simple success message
        return jsonify({
            "symbol": symbol,
            "status": "success",
            "message": f"Forecast data retrieved for {symbol}",
            "data": "Implement database query here to retrieve actual forecast data"
        })
    except Exception as e:
        return jsonify({
            "symbol": symbol,
            "status": "error",
            "error": str(e)
        }), 500

@app.route('/', methods=['GET'])
def index():
    """Home page with instructions."""
    html = '<html>'
    html += '<head><title>Financial Data Pipeline API</title>'
    html += '<style>body{font-family:Arial,sans-serif;margin:40px;line-height:1.6;}'
    html += 'code{background:#f4f4f4;padding:2px 5px;}'
    html += 'pre{background:#f4f4f4;padding:10px;border-radius:5px;}</style></head>'
    html += '<body><h1>Financial Data Pipeline API</h1>'
    html += '<p>Use this API to run financial data collection and analysis scripts.</p>'
    html += '<h2>Available Endpoints:</h2><ul>'
    html += '<li><code>GET /health</code> - Check if the service is running</li>'
    html += '<li><code>POST /run-pipeline</code> - Run the complete orchestration pipeline for a ticker</li>'
    html += '<li><code>GET /job-status/{job_id}</code> - Check the status of a running job</li>'
    html += '<li><code>GET /get-forecast/{symbol}</code> - Get forecast data for a symbol</li>'
    html += '<li><code>POST /run-script</code> - Run a specific script (legacy endpoint)</li>'
    html += '</ul>'
    
    html += '<h2>Example Usage (Full Pipeline):</h2>'
    html += '<pre>POST /run-pipeline\nContent-Type: application/json\n\n{'
    html += '"symbol": "AAPL"}</pre>'
    
    html += '</body></html>'
    
    return html

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
