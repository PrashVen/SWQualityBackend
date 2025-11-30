import datetime

# --- Database Simulations (Global State) ---
# Raw time-series data storage (for completeness, though not strictly used here)
RAW_DB = {} 
# Final aggregated data storage, optimized for dashboard queries
SUMMARY_DB = {} 
# -------------------------------------------

def aggregate_metrics(raw_data):
    """
    Calculates and saves aggregated metrics for the latest build based on raw_data.
    This function is called by the worker_processor.
    """
    
    print("\n--- 3. AGGREGATION SERVICE (CALCULATING DYNAMICALLY) ---")
    
    build_id = raw_data.get('build_id', 'unknown-build')
    timestamp = datetime.datetime.now().isoformat()
    
    # --- CALCULATE CODE COVERAGE ---
    # Using 'total_lines' and 'covered_lines' keys as found in typical CI reports
    total_lines = raw_data.get('total_lines', 1) 
    lines_covered = raw_data.get('covered_lines', 0) 
    
    # Ensure total_lines is not zero to prevent ZeroDivisionError
    if total_lines == 0:
        coverage_value = 0.0
    else:
        coverage_value = (lines_covered / total_lines) * 100
    
    # --- CALCULATE PIPELINE DURATION ---
    # Assuming duration might be in milliseconds (pipelineDurationMs) and converting to seconds
    duration_ms = raw_data.get('pipelineDurationMs', 0)
    duration_value = duration_ms / 1000.0 if duration_ms else raw_data.get('pipeline_duration_s', 0)

    # Store the final summary record
    SUMMARY_DB[build_id] = {
        'build_id': build_id,
        'timestamp': timestamp,
        'metrics': {
            'line_code_coverage': {
                'value': round(coverage_value, 2),
                'unit': 'percentage',
                'description': 'Code coverage percentage.'
            },
            'pipeline_duration_s': {
                'value': round(duration_value, 2),
                'unit': 'seconds',
                'description': 'Total pipeline execution time.'
            }
        }
    }
    
    print(f"  [AGGREGATE OK] Summary for build {build_id} saved to SUMMARY_DB.")
    print(f"  [AGGREGATE OK] Code Coverage: {coverage_value:.2f}%")
    print("-----------------------------------------------------")
    
    return SUMMARY_DB[build_id]