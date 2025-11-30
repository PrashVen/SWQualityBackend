import subprocess
import os
import signal
import time
from flask import Flask, render_template, redirect, url_for
import sys # <-- ADDED for clean program exit

app = Flask(__name__)

# Dictionary to hold the subprocess PIDs
# Key: Service name (string), Value: Process object (subprocess.Popen)
PROCESSES = {} 

# --- SERVICE CONFIGURATION ---
SERVICES = {
    # Port 5003 is used based on the last file update.
    'ingestion': {'script': 'ingestion_service.py', 'port': 5003}, 
    'worker': {'script': 'worker_processor.py', 'port': None}, # Worker has no port
    'dashboard_api': {'script': 'dashboard_api.py', 'port': 5002},
    'watcher': {'script': 'ci_data_watcher.py', 'port': None} # Watcher has no port
}

# --- TEMPLATE CONTENT (Ensures dashboard.html exists in 'templates/') ---
DASHBOARD_HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Microservices Control Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Inter', sans-serif;
            background-color: #f4f7f9;
        }
    </style>
</head>
<body class="p-8">

    <div class="max-w-4xl mx-auto bg-white p-6 rounded-xl shadow-2xl">
        <h1 class="text-3xl font-bold text-gray-900 mb-6 border-b pb-3 flex items-center">
            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-7 h-7 mr-2 text-indigo-600">
                <path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
            </svg>
            CI/CD Pipeline Microservices Orchestrator
        </h1>

        <div class="flex space-x-4 mb-8">
            <a href="{{ url_for('control_start', service_name='all') }}" class="px-6 py-3 bg-indigo-600 text-white font-semibold rounded-lg shadow-md hover:bg-indigo-700 transition duration-150 transform hover:scale-[1.02]">
                Start All Services
            </a>
            <a href="{{ url_for('control_stop', service_name='all') }}" class="px-6 py-3 bg-red-500 text-white font-semibold rounded-lg shadow-md hover:bg-red-600 transition duration-150 transform hover:scale-[1.02]">
                Stop All Services
            </a>
        </div>

        <h2 class="text-xl font-semibold text-gray-700 mb-4">Service Status</h2>
        
        <div class="space-y-4">
            {% for name, service in services.items() %}
            <div class="flex items-center justify-between p-4 border border-gray-200 rounded-lg bg-gray-50 hover:bg-white transition duration-100 shadow-sm">
                <div class="flex flex-col">
                    <span class="font-bold text-lg text-gray-800">{{ name|replace('_', ' ')|title }}</span>
                    {% if service.port %}
                        <span class="text-sm text-gray-500">Port: {{ service.port }}</span>
                    {% endif %}
                    {% if service.pid %}
                        <span class="text-xs text-gray-400">PID: {{ service.pid }}</span>
                    {% endif %}
                </div>
                
                <div class="flex items-center space-x-4">
                    <span class="px-3 py-1 text-sm font-medium rounded-full {{ service.status|status_color }}">
                        {{ service.status }}
                    </span>
                    
                    {% if service.status == 'Running' %}
                        <a href="{{ url_for('control_stop', service_name=name) }}" class="p-2 text-sm bg-yellow-500 text-white rounded-md hover:bg-yellow-600 transition duration-150" title="Stop Service">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-5 h-5">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25v13.5m-7.5-13.5v13.5" />
                            </svg>
                        </a>
                    {% else %}
                        <a href="{{ url_for('control_start', service_name=name) }}" class="p-2 text-sm bg-green-500 text-white rounded-md hover:bg-green-600 transition duration-150" title="Start Service">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke-width="2" stroke="currentColor" class="w-5 h-5">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M5.25 5.653c0-.856.917-1.353 1.636-.935l9.75 5.653c.719.418.719 1.48 0 1.898l-9.75 5.653c-.719.418-1.636-.08-1.636-.935V5.653z" />
                            </svg>
                        </a>
                    {% endif %}
                </div>
            </div>
            {% endfor %}
        </div>
        
        <div class="mt-8 pt-4 border-t text-sm text-gray-600">
            <h3 class="font-semibold mb-2">Instructions:</h3>
            <ol class="list-decimal list-inside space-y-1">
                <li>Click "Start All Services" to begin. The Ingestion API will run on port **5003** and the Dashboard API on port 5002.</li>
                <li>Edit the `ci_run_data.json` file and save it. The Watcher service will detect the change.</li>
                <li>The Watcher calls the Ingestion API, which queues the data to Redis.</li>
                <li>The Worker pulls the data from Redis, calculates the metrics, and stores the summary.</li>
                <li>The Dashboard API (port 5002) is then ready to serve the latest summary metrics.</li>
            </ol>
            {% if services.dashboard_api.status == 'Running' %}
                <p class="mt-4 font-semibold text-indigo-600">
                    Data Endpoint: <a href="http://127.0.0.1:5002/api/build-summary" target="_blank" class="hover:underline">http://127.0.0.1:5002/api/build-summary</a>
                </p>
            {% endif %}
        </div>

    </div>
</body>
</html>
"""

def start_service(service_name):
    """Starts a single service in a new subprocess. Stops any previously failed instance first."""
    config = SERVICES[service_name]
    script = config['script']
    
    # 1. Check if a process object exists and is still running
    current_process = PROCESSES.get(service_name)
    if current_process and current_process.poll() is None:
        print(f"[ORCHESTRATOR] {service_name} is already running (PID: {current_process.pid}).")
        return True
    
    # 2. If a process object exists but has stopped (poll() != None), clean it up before restart
    if current_process and current_process.poll() is not None:
        print(f"[ORCHESTRATOR] {service_name} was previously stopped/failed. Cleaning up PID {current_process.pid}.")
        del PROCESSES[service_name]

    try:
        # Start the script using the Python interpreter
        # stderr is redirected to stdout for simpler logging in the main process if needed
        process = subprocess.Popen(['python', script], preexec_fn=os.setsid, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        PROCESSES[service_name] = process
        print(f"[ORCHESTRATOR] {service_name.upper()} STARTED (PID: {process.pid})")
        return True
    except FileNotFoundError:
        print(f"[ORCHESTRATOR] ERROR: Python or {script} not found.")
        return False
    except Exception as e:
        print(f"[ORCHESTRATOR] ERROR starting {service_name}: {e}")
        return False

def stop_service(service_name):
    """Stops a single service subprocess."""
    process = PROCESSES.get(service_name)
    if process and process.poll() is None:
        try:
            # Use os.killpg to kill the process group, ensuring Flask/Watchdog sub-processes are also stopped
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            process.wait(timeout=5) # Wait for the process to terminate
            del PROCESSES[service_name]
            print(f"[ORCHESTRATOR] {service_name.upper()} STOPPED (PID: {process.pid})")
            return True
        except ProcessLookupError:
            # Process was already gone
            if service_name in PROCESSES:
                del PROCESSES[service_name]
            return True
        except Exception as e:
            print(f"[ORCHESTRATOR] ERROR stopping {service_name}: {e}")
            return False
    # If process is already stopped or doesn't exist in PROCESSES
    if service_name in PROCESSES and process.poll() is not None:
        del PROCESSES[service_name]
    return False

def get_service_status():
    """Returns the current status (Running/Stopped/Failed) for all services."""
    statuses = {}
    
    # Iterate over a copy of keys to allow deletion during iteration
    for name in list(SERVICES.keys()):
        process = PROCESSES.get(name)
        
        if process:
            exit_code = process.poll()
            
            if exit_code is None:
                # Process is running
                statuses[name] = {'status': 'Running', 'pid': process.pid, 'port': SERVICES[name].get('port')}
            else:
                # Process has terminated (Failed or Stopped)
                del PROCESSES[name] # Clean up the dead process
                
                if exit_code != 0:
                    # Non-zero exit code indicates an error (e.g., ImportError, Port Conflict)
                    # Note: We can't distinguish between Port Conflict (1) and ImportError (1)
                    statuses[name] = {'status': 'Failed (Code: {})'.format(exit_code), 'pid': None, 'port': SERVICES[name].get('port')}
                else:
                    # Exit code 0 means clean exit (e.g., manually stopped)
                    statuses[name] = {'status': 'Stopped', 'pid': None, 'port': SERVICES[name].get('port')}
        else:
            # Process was never started or was cleaned up
            statuses[name] = {'status': 'Stopped', 'pid': None, 'port': SERVICES[name].get('port')}

    return statuses

# --- FLASK ROUTES ---

@app.route('/')
def index():
    """Renders the HTML control dashboard."""
    status = get_service_status()
    # Now that we ensure dashboard.html exists in templates/, this should work
    return render_template('dashboard.html', services=status)

@app.route('/control/<service_name>/start')
def control_start(service_name):
    """API endpoint to start a service or all services."""
    if service_name == 'all':
        print("\n--- ORCHESTRATOR: Starting Full Pipeline Sequentially ---")
        # Ensure we start them in order: Worker (for Redis init), Ingestion, Dashboard, Watcher
        ordered_services = ['worker', 'ingestion', 'dashboard_api', 'watcher']
        for name in ordered_services:
            # Check status and clean up if needed before starting
            get_service_status() 
            start_service(name)
    else:
        # Check status and clean up if needed before starting
        get_service_status()
        start_service(service_name)
    
    # Give services a moment to start up and bind ports
    time.sleep(1.5) 
    return redirect(url_for('index'))

@app.route('/control/<service_name>/stop')
def control_stop(service_name):
    """API endpoint to stop a service or all services."""
    if service_name == 'all':
        print("\n--- ORCHESTRATOR: Stopping All Services ---")
        for name in reversed(list(SERVICES.keys())): # Stop in reverse order (optional but clean)
             stop_service(name)
        print("--- ORCHESTRATOR: All services stopped. ---")
    else:
        stop_service(service_name)
    return redirect(url_for('index'))

# --- HTML Template for the Dashboard UI ---

@app.template_filter('status_color')
def status_color_filter(status):
    """Helper filter to map status to Tailwind color class."""
    if status == 'Running':
        return 'bg-green-100 text-green-800'
    elif 'Failed' in status:
        return 'bg-red-100 text-red-800'
    else:
        return 'bg-gray-100 text-gray-800'

def initialize_templates():
    """Ensures the 'templates' directory and 'dashboard.html' exist."""
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    template_path = os.path.join(template_dir, 'dashboard.html')

    if not os.path.exists(template_dir):
        os.makedirs(template_dir)
        print(f"[ORCHESTRATOR] Created directory: {template_dir}")

    if not os.path.exists(template_path):
        try:
            with open(template_path, 'w') as f:
                f.write(DASHBOARD_HTML_CONTENT)
            print(f"[ORCHESTRATOR] Created template: {template_path}")
        except Exception as e:
            print(f"[ORCHESTRATOR] ERROR writing template file: {e}")

# Function to perform cleanup on exit
def cleanup_on_exit(signum, frame):
    """Signal handler for SIGINT (Ctrl+C) to stop all child processes."""
    print("\n--- ORCHESTRATOR: Received Ctrl+C (SIGINT). Stopping all running services... ---")
    
    # Iterate over a copy of the keys to avoid issues if stop_service modifies PROCESSES
    for name in list(PROCESSES.keys()):
        stop_service(name)
        
    print("--- ORCHESTRATOR: Cleanup complete. Exiting. ---")
    sys.exit(0)


if __name__ == '__main__':
    # Step 1: Initialize the templates directory and file
    initialize_templates()
    
    # Step 2: Ensure all previous processes are stopped (initial cleanup)
    for name in SERVICES:
        # Check status and clean up any processes still lingering from a previous failed run
        stop_service(name) 
        
    # Step 3: Register the SIGINT handler to ensure cleanup on Ctrl+C
    signal.signal(signal.SIGINT, cleanup_on_exit)
        
    print(f"\n--- Running Control Dashboard on http://127.0.0.1:5001 ---")
    try:
        # Running in a new thread will prevent the process from hanging if the main thread is blocked
        app.run(debug=False, port=5001)
    except Exception as e:
        print(f"FATAL ERROR: Control Dashboard failed to start: {e}")