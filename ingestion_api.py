import json
from tenacity import retry, stop_after_attempt, wait_exponential

# --- SIMULATED INFRASTRUCTURE ---
RAW_QUEUE = [] # Simulates Kafka/RabbitMQ
RAW_CI_DATA = {
    # ... (RAW_CI_DATA content remains the same) ...
    "jobId": "proj-auth-001",
    "buildNumber": 45,
    "startTime": 1709280000,
    "pipelineDurationMs": 155000,
    "reportVersion": "SQ-5.1",
    "covered_lines": 925,
    "total_lines": 1000,
    "coverageThresholdPassed": True
}

# --- FIX: Global counter for simple failure simulation ---
ATTEMPT_COUNTER = 0

# Solves Problem F: Resilience (Retry Policy)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def push_to_queue(data):
    """Simulates pushing raw data to the Message Queue/Broker with retries."""
    global ATTEMPT_COUNTER
    ATTEMPT_COUNTER += 1
    
    # Simulate a transient network error on the first attempt (Problem F)
    # The first time this runs, ATTEMPT_COUNTER will be 1, forcing a failure.
    if ATTEMPT_COUNTER <= 1: 
        print(f"  [INGESTION F] SIMULATING QUEUE FAILURE (Attempt {ATTEMPT_COUNTER}/3, RETRYING...)")
        # Ensure the counter resets only on successful push if needed, but for simplicity here, 
        # we let tenacity manage retries and fail only once.
        raise ConnectionError("Queue connection failed temporarily.")
    
    RAW_QUEUE.append(data)
    print(f"  [INGESTION OK] Pushed build {data['buildNumber']} to queue. Queue size: {len(RAW_QUEUE)}")
    return {"status": "success", "message": "Data accepted and queued"}, 202

# --- API ENDPOINT SIMULATION (rest of the file remains the same) ---
def receive_webhook(raw_data):
    # ... (function body remains the same) ...
    print("\n--- 1. INGESTION API (RECEIVE) ---")
    
    if 'buildNumber' not in raw_data:
        return {"status": "error", "message": "Missing build number"}, 400

    try:
        # Push message to Queue (The resilient step)
        response, status = push_to_queue(raw_data)
        return response, status
    except Exception as e:
        print(f"  [INGESTION ERROR] Failed after all retries: {e}")
        return {"status": "error", "message": "Queue Push failed after retries"}, 500

# if __name__ == '__main__': ... (removed for brevity, keep the original content)