# Import the json module to be able to use json.dumps()
import json
# Import all necessary functions and shared data containers from the separate files
from ingestion_api import receive_webhook, RAW_QUEUE, RAW_CI_DATA
from normalization_worker import run_normalization, RAW_DB
from aggregation_service import calculate_metrics, serve_dashboard_metric, SUMMARY_DB

if __name__ == "__main__":
    print("=====================================================================")
    print("🚀 STARTING DECOUPLED BACKEND ARCHITECTURE EXECUTION 🚀")
    print("=====================================================================")
    
    # 1. Simulate API POST (Ingestion) - Resilience Test (Problem F)
    # The queue is initialized in ingestion_api.py with a dummy to force the retry test.
    receive_webhook(RAW_CI_DATA)

    # Transfer data from ingestion_api to normalization_worker's shared raw queue/data
    
    # 2. Normalization Worker consumes the queue (Problem D)
    normalized_data = run_normalization(RAW_QUEUE)
    
    # Transfer the resulting normalized data to the aggregation service's RAW_DB
    if normalized_data:
        RAW_DB[normalized_data['build_id']] = normalized_data
        
        # 3. Aggregation Service runs calculation (Problem B)
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