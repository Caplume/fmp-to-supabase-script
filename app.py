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
    
    try:
        # Use the renamed script
        script_path = os.path.join(os.path.dirname(__file__), "scrape_articles_supabase_render.py")
        
        # Run the script with the symbol as a command-line argument
        command = [sys.executable, script_path, symbol]
        process = subprocess.run(
            command,
            capture_output=True,
            text=True
        )
        
        return jsonify({
            "symbol": symbol,
            "status": "success" if process.returncode == 0 else "error",
            "output": process.stdout,
            "error": process.stderr
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
    html += '<head><title>Financial Data Scripts API</title>'
    html += '<style>body{font-family:Arial,sans-serif;margin:40px;line-height:1.6;}'
    html += 'code{background:#f4f4f4;padding:2px 5px;}'
    html += 'pre{background:#f4f4f4;padding:10px;border-radius:5px;}</style></head>'
    html += '<body><h1>Financial Data Scripts API</h1>'
    html += '<p>Use this API to run financial data collection and analysis scripts.</p>'
    html += '<h2>Available Endpoints:</h2><ul>'
    html += '<li><code>GET /health</code> - Check if the service is running</li>'
    html += '<li><code>POST /run-script</code> - Run a script with a ticker symbol</li>'
    html += '</ul><h2>Example Usage:</h2>'
    html += '<pre>POST /run-script\nContent-Type: application/json\n\n{'
    html += '"symbol": "AAPL"}</pre></body></html>'
    
    return html

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
