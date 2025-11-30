# ci_data_watcher.py (watcher service)
import time, os, hashlib, requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

WEBHOOK_URL = "http://127.0.0.1:5003/webhook/ci"
TARGET_FILE = "ci_run_data.json"

_last_hash = None
_last_trigger = 0

class StableFileHandler(FileSystemEventHandler):
    def __init__(self, settle_time=0.6, debounce=0.5):
        super().__init__()
        self.settle_time = settle_time
        self.debounce = debounce

    def compute_hash(self, path):
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

        # 1) Give the FS a moment to finish bursts of writes
        # Poll until the file hash stabilizes or timeout
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

        print(f"[WATCHER PID {os.getpid()}] Detected real change — posting to webhook...")
        try:
            resp = requests.post(WEBHOOK_URL, json={"file": TARGET_FILE}, timeout=5)
            print("Webhook status:", resp.status_code, resp.text[:200])
        except Exception as e:
            print("Webhook error:", e)


if __name__ == "__main__":
    handler = StableFileHandler(settle_time=1.0, debounce=0.6)
    obs = Observer()
    obs.schedule(handler, ".", recursive=False)
    obs.start()
    print(f"[WATCHER START] PID {os.getpid()}, watching {os.getcwd()}/{TARGET_FILE}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()
