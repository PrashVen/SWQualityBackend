# --- SIMULATED INFRASTRUCTURE ---
RAW_DB = {} # Simulates InfluxDB/Raw Metrics Storage

def normalize_data(raw_data):
    """
    Implements the Adapter Pattern for normalization.
    Cleans raw code coverage data and maps it to the internal schema.
    """
    
    print(f"  [NORMALIZE] Processing build {raw_data['buildNumber']}...")

    # Adapter Logic: Transform and standardize fields
    normalized = {
        "project_id": raw_data['jobId'],
        "build_id": raw_data['buildNumber'],
        "timestamp": raw_data['startTime'],
        
        "pipeline_duration_s": raw_data['pipelineDurationMs'] / 1000,
        
        "covered_lines": raw_data.get('covered_lines', 0),
        "total_lines": raw_data.get('total_lines', 0),
        "coverage_threshold_passed": raw_data.get('coverageThresholdPassed', False)
    }
    
    # Store Normalized Data (Simulates writing to Raw Metrics DB)
    RAW_DB[normalized['build_id']] = normalized
    print(f"  [NORMALIZE OK] Stored normalized line-count data for build {normalized['build_id']}.")
    return normalized

# ----------------------------------------------------------------------
# UPDATED WORKER FUNCTION: It now receives the 'raw_data' (the job) directly, 
# as the queue consuming is handled by worker_processor.py (using Redis).
# ----------------------------------------------------------------------
def run_normalization(raw_data): 
    """Worker function that processes the received job."""
    print("\n--- 2. NORMALIZATION SERVICE (WORKER) ---")
    
    # If raw_data is None, it means the queue was empty.
    if raw_data is None: 
        return None
    
    # Since we already have the data, we just pass it to the processing function.
    return normalize_data(raw_data)

if __name__ == '__main__':
    print("Run the main execution flow via the worker processor for testing.")