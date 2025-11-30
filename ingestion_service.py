import redis
import json
import sys
import os
from flask import Flask, request, jsonify

# --- Configuration (Shared Globals) ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_QUEUE = 'ci_data_queue'
# --------------------------------------

# --- Redis Setup ---
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    r.ping()
    print("✅ Redis connection established successfully.")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")

app = Flask(__name__)

@app.route('/webhook/ci', methods=['POST'])
def ci_webhook():
    """
    Accepts incoming CI/CD run data and queues it to Redis.
    """
    if not request.is_json:
        return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

    raw_data = request.get_json()
    
    build_id = raw_data.get('build_id') or raw_data.get('buildNumber')
    if not build_id:
        return jsonify({"status": "error", "message": "Missing required field: build_id or buildNumber"}), 400

    raw_data['build_id'] = str(build_id)
    
    try:
        data_json = json.dumps(raw_data)
        r.lpush(REDIS_QUEUE, data_json)
        
        print(f"\n[INGESTION OK] Received build {raw_data['build_id']} and queued to Redis.")
        return jsonify({"status": "success", "message": "Data queued successfully", "build_id": raw_data['build_id']}), 200 

    except redis.exceptions.ConnectionError:
        print("❌ Redis connection lost during push.")
        return jsonify({"status": "error", "message": "Redis service unavailable"}), 503
    except Exception as e:
        print(f"❌ An unexpected error occurred during ingestion: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print(f"\n--- Running Ingestion API on http://127.0.0.1:5000/webhook/ci ---")
    try:
        app.run(debug=False, port=5000)
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"FATAL ERROR: Port 5000 is already in use. Terminating Ingestion service.")
            sys.exit(1)
        else:
            raise