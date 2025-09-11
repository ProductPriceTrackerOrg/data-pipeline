"""
Quick test to verify the global .env file loading
"""
import os
from dotenv import load_dotenv

# Load environment variables from the global .env file (go up 3 levels to data-pipeline root)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
print(f"Looking for .env file at: {env_path}")
print(f"File exists: {os.path.exists(env_path)}")

load_dotenv(env_path)

# Test if we can access the Azure connection string
azure_conn = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
airflow_uid = os.getenv("AIRFLOW_UID")

print(f"AIRFLOW_UID found: {'Yes' if airflow_uid else 'No'}")
print(f"AZURE_STORAGE_CONNECTION_STRING found: {'Yes' if azure_conn else 'No'}")

if azure_conn:
    # Only show first 50 characters for security
    print(f"Azure connection string preview: {azure_conn[:50]}...")
