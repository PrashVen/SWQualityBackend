import time
import json
import redis
from flask import Flask, jsonify
from flask_cors import CORS

# --- Configuration ---
# Flask App Setup
app = Flask(__name__)
# Enable CORS for all routes to allow frontend dashboard to access the API
CORS(app) 
API_PORT = 5002
# Redis Configuration (Assumes Redis is running locally on default port)
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
# This key MUST match the key used by worker_processor.py
SUMMARY_KEY = 'build_summary' 
# Expose REDIS_KEY so worker_processor can import it (for flexibility)
REDIS_KEY = SUMMARY_KEY 

# --- Initialization ---
try:
    # Use decode_responses=True so that strings are returned instead of bytes
    # IMPORTANT: The worker uses DB 0, so the API must use DB 0 to read the data.
    redis_db = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    # Ping to check connection
    redis_db.ping()
    print(f"[DASHBOARD API] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT} (DB 0)")
except Exception as e:
    print(f"[DASHBOARD API] ERROR: Could not connect to Redis. Ensure Redis server is running. Error: {e}")
    redis_db = None

# --- API Endpoints ---

@app.route('/api/build-summary', methods=['GET'])
def get_build_summary():
    """Fetches the latest CI metrics summary from Redis."""
    if not redis_db:
        return jsonify({"error": "Database connection failed."}), 503

    try:
        # Get the JSON string stored by the worker using the correct key
        summary_data_json = redis_db.get(SUMMARY_KEY)

        if summary_data_json:
            # Parse the JSON string back into a Python dictionary
            summary_data = json.loads(summary_data_json)
            print(f"[DASHBOARD API] Served summary data for build {summary_data.get('build_id')}")
            return jsonify(summary_data)
        else:
            print("[DASHBOARD API] No summary data found in Redis.")
            return jsonify({
                "message": "No build summary data available. Run the pipeline first.",
                "build_id": None,
                "timestamp": time.time()
            }), 404 # Returning 404 for 'Not Found' data is appropriate here

    except Exception as e:
        print(f"[DASHBOARD API] An error occurred fetching or parsing data: {e}")
        return jsonify({"error": "Internal server error during data retrieval."}), 500

# --- Service Execution ---

if __name__ == '__main__':
    # This block is executed when run directly (for testing/debugging)
    print(f"[DASHBOARD API] Starting service on port {API_PORT}...")
    try:
        # Host on 0.0.0.0 to make it accessible to other services if necessary
        app.run(debug=False, host='0.0.0.0', port=API_PORT)
    except Exception as e:
        print(f"[DASHBOARD API] FATAL ERROR: Dashboard API failed to start on port {API_PORT}. Is the port in use? Error: {e}")