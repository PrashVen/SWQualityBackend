import redis
import time
import json
# Import the aggregation function and the necessary Redis configuration variables
from aggregation_service import aggregate_metrics 
from ingestion_service import REDIS_HOST, REDIS_PORT, REDIS_QUEUE

# Connect to Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    r.ping() 
    print("✅ Redis connection established successfully.")
except Exception as e:
    print(f"❌ Redis connection failed: {e}")
    if __name__ == '__main__':
        # Exit if Redis connection fails upon startup
        exit(1)

def start_worker():
    """
    Starts the worker process, continuously pulling messages from the Redis queue
    and processing them using the aggregation service.
    """
    print("=====================================================================")
    print("🚀 STARTING WORKER PROCESSOR (Redis Consumer) 🚀")
    print("=====================================================================")
    print("Worker is now polling Redis continuously. (Press Ctrl+C to stop)")
    
    while True:
        try:
            # Blocking pop (brpop) waits up to 1 second for a message.
            # message is a tuple (queue_name_bytes, data_bytes)
            message = r.brpop(REDIS_QUEUE, timeout=1) 
            
            if message:
                # Decode the raw message bytes and parse the JSON string
                raw_data_json = message[1].decode('utf-8')
                raw_data = json.loads(raw_data_json)
                
                print(f"\n[WORKER] Message pulled from Redis. Build ID: {raw_data.get('build_id')}")
                
                # --- 2. NORMALIZATION STAGE (Minimal in worker) ---
                # We assume basic normalization occurred in the ingestion service, 
                # but any further steps would go here.
                normalized_data = raw_data 
                print("[WORKER] Normalization complete.")
                
                # --- 3. AGGREGATION STAGE ---
                # Call the function imported from aggregation_service.py
                aggregate_metrics(normalized_data)
                
                print("[WORKER] Message processed. Ready for next message.")
                
            else:
                # Timeout occurred (no messages in 1 second), continue polling
                pass
                
        except redis.exceptions.ConnectionError:
            print("❌ Redis Connection Error. Retrying in 5 seconds.")
            time.sleep(5)
        except json.JSONDecodeError as e:
            print(f"❌ JSON Decode Error: {e}. Skipping message.")
        except Exception as e:
            print(f"❌ Worker Unhandled Error: {e}")
            time.sleep(1) # Wait briefly before retrying loop on unexpected errors

if __name__ == '__main__':
    start_worker()