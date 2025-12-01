import json
import os
import signal
import sys
from flask import Flask, request, jsonify
from redis import Redis, exceptions as redis_exceptions
import time

# --- CONFIGURATION (Shared Globals) ---
INGESTION_PORT = 5000
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = os.getenv('REDIS_PORT', 6379)
REDIS_DB = 0
REDIS_QUEUE = 'ci_data_queue' 

app = Flask(__name__)
redis_client = None

def init_redis(log_connection=False):
    """
    Initializes and returns the Redis client. 
    Attempts to connect/ping, and sets redis_client to None on failure.
    """
    global redis_client
    
    if redis_client:
        try:
            # Check if existing connection is alive
            redis_client.ping()
            return redis_client
        except redis_exceptions.ConnectionError:
            print("[INGESTION SERVICE] Redis connection lost, re-attempting initialization.")
            redis_client = None # Force re-initialization

    # If redis_client is None (first run or connection lost)
    try:
        temp_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, socket_timeout=5, socket_connect_timeout=5)
        temp_client.ping()
        redis_client = temp_client
        if log_connection:
            print("[INGESTION SERVICE] Successfully connected to Redis at {}:{} (DB {})".format(REDIS_HOST, REDIS_PORT, REDIS_DB))
        return redis_client
    except redis_exceptions.ConnectionError as e:
        print(f"[INGESTION SERVICE] REDIS CONNECTION FAILED: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}. Error: {e}")
        return None

@app.route('/webhook/ci', methods=['POST'])
def ci_webhook():
    """
    Receives CI run data via webhook and queues it for the worker.
    """
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400
    
    # Attempt to parse JSON payload
    try:
        data = request.get_json()
    except Exception as e:
        # If parsing fails, return 400 immediately.
        print(f"\n[INGESTION SERVICE] Error parsing JSON payload: {e}")
        return jsonify({"error": "Invalid JSON received"}), 400
    
    
    build_id = data.get('build_id') or data.get('buildNumber')
    
    # Check for the required build identifier (build_id or buildNumber)
    if not build_id:
        return jsonify({"error": "Missing required field: build_id or buildNumber"}), 400

    # Ensure build_id is consistently in the dictionary and is a string
    data['build_id'] = str(build_id)
    
    # Try to get a valid Redis connection immediately before use
    redis_conn = init_redis() 
    if redis_conn is None:
        # Return 503 Service Unavailable if Redis connection fails
        return jsonify({"error": "Redis connection unavailable. Data cannot be queued."}), 503

    try:
        # Queue the data as a JSON string
        json_data = json.dumps(data)
        redis_conn.lpush(REDIS_QUEUE, json_data) 
        print(f"\n[INGESTION SERVICE] Received build {data['build_id']} and queued to Redis.")
        
        # Must return a successful response to the watcher
        return jsonify({"status": "success", "message": "Data queued successfully", "build_id": data['build_id']}), 200

    except redis_exceptions.RedisError as e:
        # Explicitly set redis_client to None on push failure 
        print(f"[INGESTION SERVICE] REDIS ERROR during LPUSH: {e}")
        global redis_client
        redis_client = None
        return jsonify({"status": "error", "message": "Internal server error during Redis operation. Redis client invalidated."}), 500
    except Exception as e:
        # Catch any other unhandled errors
        print(f"[INGESTION SERVICE] UNHANDLED EXCEPTION in webhook: {e}")
        return jsonify({"status": "error", "message": "Internal server error. Check logs."}), 500

def signal_handler(sig, frame):
    """Graceful exit handler for the Flask app process."""
    print(f"\n[INGESTION SERVICE] Signal {sig} received. Shutting down gracefully.")
    # Use os._exit(0) to ensure the Flask process terminates cleanly for the orchestrator
    os._exit(0)

if __name__ == '__main__':
    # Initialize Redis connection on startup (log connection details here)
    init_redis(log_connection=True)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"\n--- Running Ingestion API on http://127.0.0.1:{INGESTION_PORT}/webhook/ci ---")
    
    # Flask run command will block here until termination
    try:
        app.run(host='127.0.0.1', port=INGESTION_PORT, debug=False)
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"[INGESTION SERVICE] FATAL ERROR: Port {INGESTION_PORT} is already in use. Terminating Ingestion service.")
            sys.exit(1)
        else:
            print(f"[INGESTION SERVICE] An unexpected error occurred during Flask startup: {e}")
            sys.exit(1) # Signal failure to the orchestrator