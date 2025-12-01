# ci_data_watcher.py (watcher service)
import time, os, hashlib, requests
import json 
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WEBHOOK_URL = "http://127.0.0.1:5000/webhook/ci" 
TARGET_FILE = "ci_run_data.json"

_last_hash = None
_last_trigger = 0

class StableFileHandler(FileSystemEventHandler):
    def __init__(self, settle_time=0.6, debounce=0.5):
        super().__init__()
        self.settle_time = settle_time
        self.debounce = debounce

    def compute_hash(self, path):
        """Computes SHA256 hash of the file content."""
        try:
            with open(path, "rb") as f:
                data = f.read()
            return hashlib.sha256(data).hexdigest()
        except Exception:
            return None

    def on_modified(self, event):
        # Only care about our target file
        if event.is_directory:
            return
        if not event.src_path.endswith(TARGET_FILE):
            return

        global _last_hash, _last_trigger

        # 1) Give the FS a moment to finish bursts of writes and settle hash
        stable_hash = None
        deadline = time.time() + self.settle_time
        last_seen = None
        while time.time() < deadline:
            h = self.compute_hash(TARGET_FILE)
            if h and h == last_seen:
                stable_hash = h
                break
            last_seen = h
            time.sleep(0.05)

        # If we couldn't get a stable hash, compute once more and proceed
        if stable_hash is None:
            stable_hash = self.compute_hash(TARGET_FILE)

        if stable_hash is None:
            # file unreadable (still mid-write) — ignore this event
            return

        # Debounce by time
        if time.time() - _last_trigger < self.debounce:
            return

        # Only proceed if content actually changed
        if stable_hash == _last_hash:
            return

        _last_hash = stable_hash
        _last_trigger = time.time()

        print(f"[WATCHER] Detected real change — posting to webhook...")
        try:
            with open(TARGET_FILE, 'r') as f:
                data_to_post = json.load(f)
                
            resp = requests.post(WEBHOOK_URL, json=data_to_post, timeout=5)
            
            # The ingestion service should now correctly read build_id/buildNumber
            print(f"[WATCHER] Webhook status: {resp.status_code}")
            
        except requests.exceptions.ConnectionError:
            # Log connection error more clearly
            print(f"[WATCHER] Webhook error: Connection Refused. Check if {WEBHOOK_URL} is running.")
        except json.JSONDecodeError:
            # Log if the file is invalid JSON (e.g., empty or corrupted)
            print(f"[WATCHER] Webhook error: Could not decode JSON from {TARGET_FILE}. Skipping post.")
        except Exception as e:
            # Catch other unexpected errors
            print("[WATCHER] Webhook error:", e)


if __name__ == "__main__":
    handler = StableFileHandler(settle_time=1.0, debounce=0.6)
    obs = Observer()
    obs.schedule(handler, ".", recursive=False)
    obs.start()
    print(f"[WATCHER] PID {os.getpid()}, watching {os.getcwd()}/{TARGET_FILE}")
    
    # Initialize hash by checking the file once on startup (Simulated)
    initial_hash = handler.compute_hash(TARGET_FILE)
    if initial_hash:
        _last_hash = initial_hash
        print("[WATCHER] Initial state loaded.")
        # Attempt to post the initial data to ensure the pipeline is initialized
        try:
            with open(TARGET_FILE, 'r') as f:
                data_to_post = json.load(f)
            resp = requests.post(WEBHOOK_URL, json=data_to_post, timeout=5)
            print(f"[WATCHER] Initial data loaded. Posting to webhook. Status: {resp.status_code}")
        except Exception as e:
            print(f"[WATCHER] Initial webhook post failed: {e}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()