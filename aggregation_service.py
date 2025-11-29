import json

# --- SIMULATED INFRASTRUCTURE ---
# These are external dependencies that would be injected/configured in a real environment.
RAW_DB = {}       # Simulates InfluxDB/Raw Metrics Storage (Read-Only)
SUMMARY_DB = {}   # Simulates PostgreSQL/Summary Database (Write-Target)

def load_metrics_config(file_path='metrics_config.json'):
    """Loads the single source of truth for metric definitions."""
    try:
        # In a production system, this config would be loaded from a service like Consul or Vault.
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Metric configuration file not found at {file_path}. Cannot proceed.")
        return None

def calculate_metrics(normalized_record):
    """
    Calculates final metrics based on the dynamically loaded configuration.
    
    This function implements the heavy, asynchronous computation (Problem G)
    and enforces the single metric definition (Problem B) by evaluating the formula string.
    """
    
    print(f"\n--- 3. AGGREGATION SERVICE (CALCULATING DYNAMICALLY) ---")
    
    # Load configuration dynamically
    config = load_metrics_config()
    if not config:
        return None
        
    summary = {"build_id": normalized_record['build_id'], "project_id": normalized_record['project_id']}
    
    # Define the context (the available variables) for the formula execution.
    # This ensures the formula cannot access global variables, only the data we provide.
    context = normalized_record.copy()
    
    for metric_key, definition in config['metrics'].items():
        formula_str = definition['formula']
        
        try:
            # --- Safety Check: Prevent Division by Zero ---
            # Checks if the formula uses total_lines and if its value is zero.
            if 'total_lines' in formula_str and context.get('total_lines', 1) == 0:
                 result = 0.0
            else:
                 # WARNING: eval() is used here for prototyping simplicity. 
                 # Production code should use safer expression parsers or ast module.
                 # The second argument {"__builtins__": None} disables all Python built-in functions.
                 result = eval(formula_str, {"__builtins__": None}, context)
            
            # Store the result, rounding to 2 decimal places
            summary[metric_key] = round(result, 2)
            unit = definition.get('unit', '')
            print(f"  [AGGREGATE OK] Metric '{metric_key}' calculated: {summary[metric_key]}{unit}")
            
        except Exception as e:
            # Handle cases where the formula references a key that doesn't exist in the record.
            print(f"  [AGGREGATE ERROR] Failed to calculate metric {metric_key} using formula '{formula_str}': {e}")
            summary[metric_key] = None

    # Store in Summary DB
    SUMMARY_DB[normalized_record['build_id']] = summary
    return summary

def serve_dashboard_metric():
    """
    Returns the final, pre-calculated metric for the UI (Problem G).
    This simulates the fast, read-only query that the Presentation API would make.
    """
    
    print("\n--- 4. UI API LAYER (FAST QUERY) ---")
    if not SUMMARY_DB:
        return {"status": "error", "message": "No data available"}
    
    # Retrieve the latest pre-calculated metric
    latest_id = max(SUMMARY_DB.keys())
    metric = SUMMARY_DB[latest_id]
    
    # Retrieve the specific metric needed by the UI
    return {
        "status": "success",
        "latest_build_id": metric['build_id'],
        "metric_name": "Line Code Coverage",
        "value": metric.get('line_code_coverage', None),
        "unit": "%",
        "duration_s": metric.get('pipeline_duration_s', None)
    }

if __name__ == '__main__':
    print("This file contains the logic for the Aggregation and Presentation services.")
    print("Run the main `backend_execution.py` script to see the full orchestrated flow.")