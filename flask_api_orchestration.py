from flask import Flask, request, jsonify
import subprocess
import threading
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api_server.log"),
        logging.StreamHandler()
    ]
)

app = Flask(__name__)

# Store job status
job_status = {}

def run_orchestration_job(job_id, symbol):
    """Run the orchestration script as a background job"""
    try:
        # Update job status to running
        job_status[job_id] = {
            "status": "running",
            "symbol": symbol,
            "start_time": datetime.now().isoformat(),
            "message": f"Started financial pipeline for {symbol}"
        }
        
        # Run the orchestration script
        logging.info(f"Starting orchestration script for {symbol}, job ID: {job_id}")
        result = subprocess.run(
            ["python", "orchestration-script.py", symbol],
            capture_output=True,
            text=True
        )
        
        # Check if the script was successful
        if result.returncode == 0:
            job_status[job_id] = {
                "status": "completed",
                "symbol": symbol,
                "end_time": datetime.now().isoformat(),
                "message": f"Financial pipeline completed for {symbol}"
            }
            logging.info(f"Orchestration completed successfully for {symbol}, job ID: {job_id}")
        else:
            job_status[job_id] = {
                "status": "failed",
                "symbol": symbol,
                "end_time": datetime.now().isoformat(),
                "message": f"Financial pipeline failed for {symbol}",
                "error": result.stderr
            }
            logging.error(f"Orchestration failed for {symbol}, job ID: {job_id}")
            logging.error(f"Error: {result.stderr}")
    
    except Exception as e:
        # Handle any exceptions
        job_status[job_id] = {
            "status": "failed",
            "symbol": symbol,
            "end_time": datetime.now().isoformat(),
            "message": f"Exception during execution: {str(e)}"
        }
        logging.error(f"Exception during orchestration for {symbol}, job ID: {job_id}: {str(e)}")

@app.route('/trigger-pipeline', methods=['POST'])
def trigger_pipeline():
    """API endpoint to trigger the financial pipeline"""
    # Get the stock symbol from the request
    data = request.json
    symbol = data.get('symbol', 'AAPL')  # Default to AAPL if no symbol provided
    
    # Generate a job ID
    job_id = f"{symbol}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Start the job in a background thread
    thread = threading.Thread(target=run_orchestration_job, args=(job_id, symbol))
    thread.daemon = True
    thread.start()
    
    # Return the job ID immediately
    return jsonify({
        "status": "started",
        "job_id": job_id,
        "message": f"Financial pipeline started for {symbol}"
    })

@app.route('/job-status/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """API endpoint to check job status"""
    if job_id in job_status:
        return jsonify(job_status[job_id])
    else:
        return jsonify({
            "status": "not_found",
            "message": f"No job found with ID: {job_id}"
        }), 404

@app.route('/get-forecast/<symbol>', methods=['GET'])
def get_forecast(symbol):
    """API endpoint to retrieve forecast data for a symbol"""
    try:
        import psycopg2
        import json
        
        # Database connection parameters (use environment variables in production)
        DB_NAME = os.environ.get("DB_NAME", "postgres")
        DB_USER = os.environ.get("DB_USER", "postgres")
        DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
        DB_HOST = os.environ.get("DB_HOST", "db.aybqlqgrbcxxuvmuibdx.supabase.co")
        DB_PORT = os.environ.get("DB_PORT", "5432")
        
        # Connect to the database
        conn_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        # Query for the forecast data
        cur.execute("""
            SELECT metric, year, case_type, value, rationale 
            FROM final_forecasts 
            WHERE symbol = %s
            ORDER BY metric, year
        """, (symbol,))
        
        # Fetch all results
        forecast_rows = cur.fetchall()
        
        # Close the database connection
        cur.close()
        conn.close()
        
        # Format the results
        forecasts = []
        for row in forecast_rows:
            forecasts.append({
                "metric": row[0],
                "year": row[1],
                "case_type": row[2],
                "value": row[3],
                "rationale": row[4]
            })
        
        return jsonify({
            "symbol": symbol,
            "forecasts": forecasts,
            "count": len(forecasts)
        })
        
    except Exception as e:
        logging.error(f"Error retrieving forecast data: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)