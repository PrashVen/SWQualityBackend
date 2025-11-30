import subprocess
import os
import signal
import time
import sys 
import atexit

# Dictionary to hold the subprocess Popen objects
# Key: Service name (string), Value: Process object (subprocess.Popen)
PROCESSES = {} 

# --- SERVICE CONFIGURATION ---
SERVICES = {
    # Worker must start first as it is the consumer
    'worker': {'script': 'worker_processor.py', 'port': None},
    # API must start before Ingestion to avoid errors
    'dashboard_api': {'script': 'dashboard_api.py', 'port': 5002},
    # Ingestion API needs to be running to receive data from the watcher
    # FIX: The logs show the ingestion service runs on port 5000, not 5003.
    'ingestion': {'script': 'ingestion_service.py', 'port': 5000}, 
    # Watcher starts last as it triggers the whole pipeline
    'watcher': {'script': 'ci_data_watcher.py', 'port': None}
}

def start_service(service_name):
    """Starts a single service in a new subprocess, redirecting output to the console."""
    config = SERVICES[service_name]
    script = config['script']
    
    # 1. Check if a process object exists and is still running
    current_process = PROCESSES.get(service_name)
    if current_process and current_process.poll() is None:
        print(f"[ORCHESTRATOR] {service_name.upper()} is already running (PID: {current_process.pid}).")
        return True
    
    # 2. If a process object exists but has stopped, clean it up
    if current_process and current_process.poll() is not None:
        del PROCESSES[service_name]

    try:
        # Start the subprocess, using stdout/stderr of the parent process
        # start_new_session=True creates a new process group, which simplifies cleanup
        process = subprocess.Popen(
            [sys.executable, script], 
            stdout=sys.stdout, # Direct child output to the parent console
            stderr=sys.stderr, # Direct child output to the parent console
            start_new_session=True 
        )
        PROCESSES[service_name] = process
        print(f"✅ STARTED {service_name.upper()} (PID: {process.pid}) running script: {script}")
        return True
    except FileNotFoundError:
        print(f"❌ ERROR: Python or script {script} not found.")
        return False
    except Exception as e:
        print(f"❌ ERROR starting {service_name}: {e}")
        return False

def stop_service(service_name):
    """Stops a single service subprocess."""
    process = PROCESSES.get(service_name)
    if process and process.poll() is None:
        try:
            # We use os.kill to terminate the process
            os.kill(process.pid, signal.SIGTERM)
            process.wait(timeout=5) # Wait for the process to terminate
            del PROCESSES[service_name]
            print(f"🛑 STOPPED {service_name.upper()} (PID: {process.pid})")
            return True
        except ProcessLookupError:
            # Process was already gone
            if service_name in PROCESSES:
                del PROCESSES[service_name]
            return True
        except Exception as e:
            print(f"❌ ERROR stopping {service_name}: {e}")
            return False
    
    # Clean up if process object exists but has stopped
    if service_name in PROCESSES and process and process.poll() is not None:
        del PROCESSES[service_name]
        return True
    
    return False

def cleanup_all():
    """Stops all running services on script exit or interrupt."""
    if not PROCESSES:
        return

    print("\n\n--- Orchestrator: Initiating graceful shutdown of all services... ---")
    
    # Stop services in reverse order (Watcher -> Ingestion -> API -> Worker)
    for name in reversed(list(SERVICES.keys())):
        stop_service(name)
        
    print("--- Orchestrator: All services stopped. ---")


if __name__ == '__main__':
    # Register the cleanup function to run on normal exit or interrupt
    atexit.register(cleanup_all)
    
    print("=====================================================")
    print("🚀 CI/CD PIPELINE ORCHESTRATOR (Command Line Mode) 🚀")
    print("=====================================================")
    
    print("\n--- Starting Full Pipeline Sequentially ---")
    
    # Start services in defined order
    for name in SERVICES.keys():
        start_service(name)
        # Add a slight delay for API services to bind ports
        if SERVICES[name].get('port'):
            time.sleep(1.5)

    print("\n-----------------------------------------------------")
    print(f"Pipeline is RUNNING. Check data at http://127.0.0.1:{SERVICES['dashboard_api']['port']}/api/build-summary")
    print("Press Ctrl+C to stop all services gracefully.")
    print("-----------------------------------------------------")

    try:
        # Keep the main process alive indefinitely until interrupted (Ctrl+C)
        while True:
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\nInterrupt received. Exiting Orchestrator...")
    # The atexit handler (cleanup_all) will run automatically here