import redis
import time
import json
import traceback # Import traceback for detailed error logging
# Import the aggregation function
from aggregation_service import aggregate_metrics 
# Import Redis connection details from the ingestion service config
from ingestion_service import REDIS_HOST, REDIS_PORT, REDIS_QUEUE

# Import the output key used by the Dashboard API
try:
    from dashboard_api import REDIS_KEY as SUMMARY_REDIS_KEY
except ImportError:
    # Fallback if dashboard_api.py is not available in the path, use the hardcoded value
    SUMMARY_REDIS_KEY = 'build_summary' 
    print(f"[WORKER] Warning: Could not import REDIS_KEY from dashboard_api. Using default: {SUMMARY_REDIS_KEY}")

# Connect to Redis
try:
    # We use db=0 for both the queue (input) and the summary (output)
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
    r.ping() 
    print("[WORKER] Redis connection established successfully.")
except Exception as e:
    print(f"[WORKER] Redis connection failed: {e}")
    # We allow the script to proceed here, as the loop handles reconnection attempts.

def start_worker():
    """
    Starts the worker process, continuously pulling messages from the Redis queue,
    processing them, and saving the final summary to Redis.
    """
    print("=====================================================================")
    print("  STARTING WORKER PROCESSOR (Redis Consumer)                         ")
    print("=====================================================================")
    print(f"Worker polling queue: {REDIS_QUEUE}. Saving results to key: {SUMMARY_REDIS_KEY}")
    print("Worker is now polling Redis continuously. (Press Ctrl+C to stop)")
    
    while True:
        try:
            # Blocking pop (brpop) waits up to 1 second for a message.
            message = r.brpop(REDIS_QUEUE, timeout=1) 
            
            if message:
                # Decode the raw message bytes and parse the JSON string
                raw_data_json = message[1].decode('utf-8')
                raw_data = json.loads(raw_data_json)
                
                print(f"\n[WORKER] Message pulled from Redis. Build ID: {raw_data.get('build_id')}")
                
                # --- 2. NORMALIZATION STAGE ---
                normalized_data = raw_data 
                print("[WORKER] Normalization complete.")
                
                # --- 3. AGGREGATION STAGE ---
                # Capture the returned summary data
                final_summary = aggregate_metrics(normalized_data)
                
                # --- 4. PERSISTENCE STAGE (THE FIX) ---
                # Serialize the dictionary and save it to the shared summary key
                summary_json_string = json.dumps(final_summary)
                r.set(SUMMARY_REDIS_KEY, summary_json_string)
                
                print(f"[WORKER] Successfully saved final summary to Redis key: {SUMMARY_REDIS_KEY}")
                print("[WORKER] Message processed. Ready for next message.")
                
            else:
                # Timeout occurred (no messages in 1 second), continue polling
                pass
                
        except redis.exceptions.ConnectionError:
            print("[WORKER] Redis Connection Error. Retrying in 5 seconds.")
            time.sleep(5)
        except json.JSONDecodeError as e:
            print(f"[WORKER] JSON Decode Error: {e}. Skipping message.")
        except Exception as e:
            print(f"[WORKER] Worker Unhandled Error: {e}")
            time.sleep(1)

if __name__ == '__main__':
    try:
        start_worker()
    except Exception as e:
        # Final catch block to log any exception that prevents the main loop from starting
        print(f"[WORKER] FATAL WORKER STARTUP ERROR: {e}")
        # Print the full traceback to the log file for diagnosis
        traceback.print_exc()
        exit(1)