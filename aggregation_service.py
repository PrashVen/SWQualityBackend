import datetime
import json
import os
import math 

# --- CONFIGURATION FILE PATH ---
CONFIG_FILE_PATH = 'metrics_config.json'

# --- Internal Constants for Simplicity ---
# Define required input keys and their defaults for safety
DEFAULT_INPUTS = {
    "total_lines": 1, 
    "covered_lines": 0, 
    "pipelineDurationMs": 0 
}
# ------------------------------------------

def load_config():
    """Loads the metrics configuration file and handles errors."""
    try:
        # Check if the file exists before trying to open it
        if not os.path.exists(CONFIG_FILE_PATH):
            raise FileNotFoundError(f"Config file not found: {CONFIG_FILE_PATH}")
            
        with open(CONFIG_FILE_PATH, 'r') as f:
            config = json.load(f)
        print(f"[CONFIG] Configuration loaded from {CONFIG_FILE_PATH}.")
        return config
        
    except FileNotFoundError as e:
        print(f"[CONFIG ERROR] {e}. Cannot start aggregation service.")
        return None
    except json.JSONDecodeError:
        print(f"[CONFIG ERROR] Failed to decode JSON from {CONFIG_FILE_PATH}. Check file syntax.")
        return None

# Load configuration globally once when the service starts
METRICS_CONFIG = load_config()

def calculate_metric_value(metric_key, config, raw_data):
    """
    Calculates a single metric value by dynamically executing the formula 
    loaded from the simplified metrics configuration using eval().
    """
    if not config or metric_key not in config['metrics']:
        print(f"[ERROR] Metric key '{metric_key}' not found in configuration.")
        return 0

    metric_def = config['metrics'][metric_key]
    
    # 1. Pull formula directly from metric definition (simplified config structure)
    formula = metric_def['formula'] 
    
    # --- Prepare Execution Scope ---
    scope = {}
    
    # 2. Populate scope with required variables from raw_data
    # This replaces the complex INPUT_MAP logic
    if metric_key == 'line_code_coverage':
        # Required variables for coverage formula: covered_lines, total_lines
        scope['covered_lines'] = raw_data.get('covered_lines', DEFAULT_INPUTS['covered_lines'])
        scope['total_lines'] = raw_data.get('total_lines', DEFAULT_INPUTS['total_lines'])

    elif metric_key == 'pipeline_duration_s':
        # Required variable for pipeline duration: duration_ms (mapped from pipelineDurationMs)
        scope['duration_ms'] = raw_data.get('pipelineDurationMs', DEFAULT_INPUTS['pipelineDurationMs'])
        # 3. Hardcoded conversion logic to fix the simple formula "pipeline_duration_s"
        formula = "duration_ms / 1000.0" 

    # --- Safety Check: Prevent division by zero ---
    if metric_key == 'line_code_coverage':
        if scope.get('total_lines', 0) == 0:
            return 0
    
    # --- Execute Dynamic Formula ---
    try:
        # CRITICAL STEP: eval() executes the string formula using the variables in the scope.
        # We restrict the environment to prevent security risks (no builtins)
        result = eval(formula, {"__builtins__": None, "math": math}, scope)
        
        # 4. Apply fixed rounding (2 decimals)
        return round(result, 2)
        
    except Exception as e:
        print(f"[FORMULA ERROR] Failed to evaluate formula for {metric_key}. Error: {e}")
        print(f"  [FORMULA DEBUG] Formula: {formula}, Scope: {scope}")
        return 0

def aggregate_metrics(raw_data):
    """
    Calculates aggregated metrics by iterating through the configuration
    and dynamically executing the formulas defined in metrics_config.json.
    """
    if not METRICS_CONFIG:
        print("[AGGREGATION] ERROR: Configuration missing. Cannot aggregate metrics.")
        return {'build_id': raw_data.get('build_id', 'unknown'), 'timestamp': datetime.datetime.now().timestamp(), 'metrics': {}}

    print("\n--- 3. AGGREGATION ENGINE (CALCULATING DYNAMICALLY) ---")
    
    # Build ID fallback logic improved to check common fields
    build_id = raw_data.get('build_id') or raw_data.get('buildNumber', 'unknown-build')
    timestamp = datetime.datetime.now().timestamp() 
    
    final_metrics = {}
    
    # Iterate over all defined metrics in the config
    for metric_key, metric_def in METRICS_CONFIG['metrics'].items():
        
        # Calculate the value using the dynamic function
        value = calculate_metric_value(metric_key, METRICS_CONFIG, raw_data)
        
        final_metrics[metric_key] = {
            'value': value,
            'unit': metric_def['unit'],
            'description': metric_def.get('description', 'No description provided in config.')
        }
        
    print(f"  [AGGREGATION] Calculated summary for build {build_id}.")
    
    # Debug line using the dynamically calculated value
    coverage_value = final_metrics.get('line_code_coverage', {}).get('value', 'N/A')
    print(f"  [AGGREGATION] Code Coverage: {coverage_value}%")
    print("-----------------------------------------------------")
    
    # Prepare the final summary record
    final_summary = {
        'build_id': build_id,
        'timestamp': timestamp,
        'metrics': final_metrics
    }
    
    # Return the summary dictionary for the worker to save
    return final_summary