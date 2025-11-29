import json
from flask import Flask # Needed for the test_request_context

# Import all necessary functions and shared data containers
from ingestion_service import RAW_QUEUE, handle_webhook
from normalization_worker import run_normalization, RAW_DB
from aggregation_service import calculate_metrics, serve_dashboard_metric, SUMMARY_DB

def load_raw_data(file_path='ci_run_data.json'):
    """Loads the input data from the dedicated JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Sample data file not found at {file_path}. Cannot proceed.")
        return None

def simulate_http_post(api_handler, data):
    """
    Simulates the CI/CD webhook POST request by running the Flask handler 
    function in a controlled test context.
    """
    
    print("\n--- 1. SIMULATING CI POST TO INGESTION API ENDPOINT ---")
    
    # We initialize a minimal Flask application context
    app = Flask(__name__)
    
    # Use Flask's testing utility to simulate an incoming HTTP POST request
    with app.test_request_context(json=data, method='POST'):
        # Call the actual route handler function
        response_obj, status_code = api_handler() 
        
        # Flask's response object needs to be converted back to JSON data for inspection
        response_data = json.loads(response_obj.get_data(as_text=True))
        
        return response_data, status_code

if __name__ == "__main__":
    print("=====================================================================")
    print("🚀 STARTING DECOUPLED BACKEND ARCHITECTURE EXECUTION (API SIMULATION) 🚀")
    print("=====================================================================")
    
    # LOAD THE EXTERNAL DATA SOURCE
    RAW_CI_DATA = load_raw_data()
    if not RAW_CI_DATA:
        exit(1) # Stop execution if data is missing

    # 1. Simulate API POST (Ingestion) - Resilience Test (Problem F)
    api_response, status_code = simulate_http_post(handle_webhook, RAW_CI_DATA)
    print(f"CI System received API Response Code: {status_code}, Message: {api_response['message']}")

    # 2. Normalization Worker consumes the queue (Problem D)
    normalized_data = run_normalization(RAW_QUEUE)
    
    # 3. Aggregation Service runs calculation (Problem B)
    if normalized_data:
        # Update shared DBs for the next service
        RAW_DB[normalized_data['build_id']] = normalized_data
        
        final_metrics = calculate_metrics(normalized_data)
        
        # 4. UI Dashboard Requests the Latest Metric (Problem G)
        dashboard_data = serve_dashboard_metric()
        
        print("\n=====================================================================")
        print("--- ✅ FINAL UI DASHBOARD DATA ---")
        print("=====================================================================")
        print(f"Source DB Size: {len(RAW_DB)} records | Summary DB Size: {len(SUMMARY_DB)} records")
        print("Data is pre-calculated and served instantly from the Summary DB.")
        print(json.dumps(dashboard_data, indent=4))
        print("=====================================================================")