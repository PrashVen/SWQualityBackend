from flask import Flask, jsonify, CORS
# CRITICAL: Import the final metrics DB. 
# This line requires aggregation_service.py to be in the same directory.
from aggregation_service import SUMMARY_DB 

app = Flask(__name__)
# Enable CORS to allow the HTML dashboard (served from 'file://' or another port) 
# to fetch data from this API.
CORS(app) 

@app.route('/api/build-summary', methods=['GET'])
def get_latest_metrics():
    """
    Retrieves the latest complete build summary record from the SUMMARY_DB.
    """
    
    print("\n--- 4. DASHBOARD API (QUERY) ---")
    
    if not SUMMARY_DB:
        print("  [DASHBOARD] SUMMARY_DB is empty.")
        # Return an empty but structured response for the UI to handle gracefully
        return jsonify({"status": "empty", "message": "No summary data available", "metrics": {}}), 200

    # Get the latest entry by accessing the last key added to the dictionary
    # In Python 3.7+, dictionary insertion order is preserved.
    latest_build_id = list(SUMMARY_DB.keys())[-1]
    latest_data = SUMMARY_DB[latest_build_id]

    print(f"  [DASHBOARD OK] Serving complete build summary for build {latest_build_id}")
    
    # Construct the final response structure
    response_data = {
        "build_id": latest_build_id,
        "timestamp": latest_data.get('timestamp'),
        "metrics": latest_data.get('metrics', {}) 
    }
    
    return jsonify(response_data)

if __name__ == '__main__':
    print(f"\n--- Running Dashboard API on http://127.0.0.1:5002 ---")
    app.run(debug=False, port=5002)