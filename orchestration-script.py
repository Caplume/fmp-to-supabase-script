import subprocess
import time
import sys
import os
import concurrent.futures
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("financial_pipeline.log"),
        logging.StreamHandler()
    ]
)

# Set the default symbol if none is provided
DEFAULT_SYMBOL = "AAPL"

def run_script(script_path, symbol, timeout=300):
    """
    Run a Python script with the given symbol as an argument.
    
    Args:
        script_path: Path to the script to run
        symbol: Stock symbol to pass to the script
        timeout: Maximum execution time in seconds
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info(f"Starting {script_path} for {symbol}")
        
        # Start the process
        process = subprocess.Popen(
            [sys.executable, script_path, symbol],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for completion with timeout
        stdout, stderr = process.communicate(timeout=timeout)
        
        # Log output
        for line in stdout.splitlines():
            logging.info(f"[{script_path}] {line}")
            
        # Log errors
        if stderr:
            for line in stderr.splitlines():
                logging.warning(f"[{script_path}] {line}")
        
        # Check return code
        if process.returncode != 0:
            logging.error(f"Script {script_path} failed with return code {process.returncode}")
            return False
            
        logging.info(f"Completed {script_path} for {symbol}")
        return True
        
    except subprocess.TimeoutExpired:
        logging.error(f"Script {script_path} timed out after {timeout} seconds")
        # Try to kill the process
        process.kill()
        return False
    except Exception as e:
        logging.error(f"Error running {script_path}: {e}")
        return False

def run_parallel_scripts(script_paths, symbol, max_workers=3, timeout=300):
    """
    Run multiple scripts in parallel.
    
    Args:
        script_paths: List of script paths to run
        symbol: Stock symbol to pass to each script
        max_workers: Maximum number of concurrent scripts
        timeout: Maximum execution time per script in seconds
        
    Returns:
        True if all scripts succeeded, False otherwise
    """
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_script = {
            executor.submit(run_script, script, symbol, timeout): script
            for script in script_paths
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_script):
            script = future_to_script[future]
            try:
                result = future.result()
                results.append(result)
                logging.info(f"Script {script} finished with result: {result}")
            except Exception as e:
                logging.error(f"Script {script} generated an exception: {e}")
                results.append(False)
    
    # Return True only if all scripts succeeded
    return all(results)

def run_financial_pipeline(symbol):
    """
    Run the complete financial data pipeline for a given symbol.
    
    Args:
        symbol: Stock symbol to analyze
        
    Returns:
        True if the pipeline completed successfully, False otherwise
    """
    start_time = time.time()
    logging.info(f"Starting financial pipeline for {symbol}")
    
    # Step 1: Data Collection (Parallel)
    step1_scripts = [
        "fetch_press_releases_render.py",
        "scrape_articles_supabase_render.py",
        "scrape_sec_filings_render.py"
    ]
    
    logging.info("Starting Step 1: Data Collection")
    step1_success = run_parallel_scripts(step1_scripts, symbol)
    
    if not step1_success:
        logging.error("Step 1 failed. Aborting pipeline.")
        return False
    
    # Wait a moment to ensure all data is committed to the database
    time.sleep(5)
    
    # Step 2: Data Analysis (Parallel)
    step2_scripts = [
        "analyze_sec_filings_render.py",
        "analyze_news_sentiment_render.py"
    ]
    
    logging.info("Starting Step 2: Data Analysis")
    step2_success = run_parallel_scripts(step2_scripts, symbol)
    
    if not step2_success:
        logging.error("Step 2 failed. Aborting pipeline.")
        return False
    
    # Wait a moment to ensure all analysis is committed
    time.sleep(5)
    
    # Step 3: Forecast Generation
    logging.info("Starting Step 3: Forecast Generation")
    step3_success = run_script("generate_comprehensive_forecast_render.py", symbol, timeout=600)  # Higher timeout for forecast
    
    if not step3_success:
        logging.error("Step 3 failed.")
        return False
    
    # Calculate total runtime
    end_time = time.time()
    total_runtime = end_time - start_time
    logging.info(f"Pipeline completed in {total_runtime:.2f} seconds")
    
    return True

if __name__ == "__main__":
    # Set up timestamp for this run
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"=== Financial Pipeline Run: {run_timestamp} ===")
    
    # Get symbol from command line or use default
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        logging.info(f"Using provided symbol: {symbol}")
    else:
        symbol = DEFAULT_SYMBOL
        logging.info(f"No symbol provided, using default: {symbol}")
    
    # Run the pipeline
    success = run_financial_pipeline(symbol)
    
    if success:
        logging.info(f"✅ Financial pipeline completed successfully for {symbol}")
        sys.exit(0)
    else:
        logging.error(f"❌ Financial pipeline failed for {symbol}")
        sys.exit(1)
