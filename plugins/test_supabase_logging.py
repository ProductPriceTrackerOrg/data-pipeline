"""
Test script to verify Supabase logging functionality without running a full DAG
"""

import os
import json
import requests
import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def test_supabase_logging():
    """Test the Supabase logging functionality"""
    
    try:
        # Get Supabase credentials from environment
        SUPABASE_URL = os.getenv('SUPABASE_URL')
        SUPABASE_KEY = os.getenv('SUPABASE_API_KEY')
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("ERROR: Supabase credentials not found in environment variables")
            return
        
        # Create test data mimicking a DAG run
        test_data = {
            "dag_id": "test_dag2",
            "run_id": 67891,
            "run_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": "failed",
            "duration_seconds": 50,
            "tasks_failed": 2,
            "error_message": "Scrapers didnt work",
            "run_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
        print(f"Sending test data to Supabase: {json.dumps(test_data, indent=2)}")
        
        # Send data to Supabase
        headers = {
            "apikey": SUPABASE_KEY,
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/pipelinerunlog",
            headers=headers,
            json=test_data
        )
        
        if response.status_code >= 300:
            print(f"ERROR: Failed to log to Supabase. Status code: {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print(f"SUCCESS: Test data successfully logged to Supabase!")
            print(f"Status code: {response.status_code}")
            
    except Exception as e:
        print(f"ERROR: Exception occurred during test: {str(e)}")

if __name__ == "__main__":
    test_supabase_logging()