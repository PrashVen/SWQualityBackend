import datetime
import json # Added import for timestamp formatting

def aggregate_metrics(raw_data):
    """
    Calculates aggregated metrics for the latest build based on raw_data
    and returns the structured summary dictionary.
    """
    
    print("\n--- 3. AGGREGATION SERVICE (CALCULATING DYNAMICALLY) ---")
    
    build_id = raw_data.get('build_id', 'unknown-build')
    # Use Unix timestamp (seconds since epoch) for standard format
    timestamp = datetime.datetime.now().timestamp() 
    
    # --- CALCULATE CODE COVERAGE ---
    total_lines = raw_data.get('total_lines', 1) 
    lines_covered = raw_data.get('covered_lines', 0) 
    
    if total_lines == 0:
        coverage_value = 0.0
    else:
        coverage_value = (lines_covered / total_lines) * 100
    
    # --- CALCULATE PIPELINE DURATION ---
    duration_ms = raw_data.get('pipelineDurationMs', 0)
    duration_value = duration_ms / 1000.0 if duration_ms else raw_data.get('pipeline_duration_s', 0)

    # Prepare the final summary record
    final_summary = {
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
    
    print(f"  [AGGREGATE OK] Calculated summary for build {build_id}.")
    print(f"  [AGGREGATE OK] Code Coverage: {coverage_value:.2f}%")
    print("-----------------------------------------------------")
    
    # Return the summary dictionary for the worker to save
    return final_summary