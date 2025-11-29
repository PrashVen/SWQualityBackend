import json
from flask import Flask, request, jsonify 
from tenacity import retry, stop_after_attempt, wait_exponential

# --- SIMULATED INFRASTRUCTURE ---
# Simulates the Message Queue (Kafka/RabbitMQ)
RAW_QUEUE = []

# --- Resilience Logic Setup ---
# Global counter used to simulate a transient network failure on the first attempt
ATTEMPT_COUNTER = 0 

app = Flask(__name__) # Initialize the Flask app

# Solves Problem F: Resilience (Retry Policy)
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def push_to_queue(data):
    """Simulates pushing raw data to the Message Queue/Broker with retries."""
    global ATTEMPT_COUNTER
    ATTEMPT_COUNTER += 1
    
    # Simulate a transient network error on the first attempt
    if ATTEMPT_COUNTER <= 1: 
        print(f"  [INGESTION F] SIMULATING QUEUE FAILURE (Attempt {ATTEMPT_COUNTER}/3, RETRYING...)")
        raise ConnectionError("Queue connection failed temporarily.")
    
    # Success path: Push data to the queue
    RAW_QUEUE.append(data)
    print(f"  [INGESTION OK] Pushed build {data['buildNumber']} to queue. Queue size: {len(RAW_QUEUE)}")
    return True

# ======================================================================
# API ENDPOINT DEFINITION
# ======================================================================

@app.route('/webhook/ci', methods=['POST'])
def handle_webhook():
    """
    Handles incoming CI webhooks (POST requests), validates data, and pushes to the queue.
    The raw data comes from the HTTP request body (request.json).
    """
    
    print("\n--- 1. INGESTION API (RECEIVE) ---")
    
    # 1. Input Validation and Security Check
    if not request.json:
        return jsonify({"status": "error", "message": "Missing JSON payload"}), 400
    
    raw_data = request.json
    
    if 'buildNumber' not in raw_data:
        return jsonify({"status": "error", "message": "Missing build number in payload"}), 400

    # 2. Push message to Queue (Resilient step)
    try:
        # The data is validated and pushed for asynchronous processing
        push_to_queue(raw_data)
        
        # Solves Problem G: FAST response (API returns immediately)
        return jsonify({"status": "success", "message": "Data accepted and queued for processing"}), 202
        
    except Exception as e:
        print(f"  [INGESTION ERROR] Failed after all retries: {e}")
        # Return a 500 status code to the CI system upon complete failure
        return jsonify({"status": "error", "message": "Queue Push failed after retries"}), 500

# Function to run the Flask app 
def run_ingestion_api():
    print(f"\n--- Running Ingestion API on http://127.0.0.1:5000/webhook/ci ---")
    app.run(debug=False, port=5000)

if __name__ == '__main__':
    run_ingestion_api()