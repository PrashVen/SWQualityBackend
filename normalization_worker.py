# --- SIMULATED INFRASTRUCTURE ---
RAW_DB = {} # Simulates InfluxDB/Raw Metrics Storage

def normalize_data(raw_data):
    """
    Implements the Adapter Pattern for normalization.
    Cleans raw code coverage data and maps it to the internal schema (Problem D).
    """
    
    print(f"  [NORMALIZE] Processing build {raw_data['buildNumber']}...")

    # Adapter Logic: Transform and standardize fields
    normalized = {
        "project_id": raw_data['jobId'],
        "build_id": raw_data['buildNumber'],
        "timestamp": raw_data['startTime'],
        
        # Standardize units: ms -> seconds
        "pipeline_duration_s": raw_data['pipelineDurationMs'] / 1000,
        
        # Normalize Coverage Fields: Extract the counts needed for aggregation
        "covered_lines": raw_data.get('covered_lines', 0),
        "total_lines": raw_data.get('total_lines', 0),
        "coverage_threshold_passed": raw_data.get('coverageThresholdPassed', False)
    }
    
    # Store Normalized Data (Simulates writing to Raw Metrics DB)
    RAW_DB[normalized['build_id']] = normalized
    print(f"  [NORMALIZE OK] Stored normalized line-count data for build {normalized['build_id']}.")
    return normalized

def run_normalization(RAW_QUEUE):
    """Simulates the background worker consuming the message queue."""
    print("\n--- 2. NORMALIZATION SERVICE (WORKER) ---")
    if not RAW_QUEUE:
        print("  [NORMALIZE] Queue is empty.")
        return None
    
    raw_data = RAW_QUEUE.pop(0) # Consume the message
    return normalize_data(raw_data)

if __name__ == '__main__':
    # This file needs the queue and raw data from ingestion_api to run independently
    print("Run the main `backend_execution.py` script to see the full flow.")