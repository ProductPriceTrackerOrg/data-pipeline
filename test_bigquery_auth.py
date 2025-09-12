#!/usr/bin/env python3
"""
Test BigQuery authentication
"""
from google.cloud import bigquery

def main():
    """Test BigQuery authentication"""
    print("Attempting to authenticate with BigQuery...")
    try:
        client = bigquery.Client()
        print(f"Successfully authenticated! Project ID: {client.project}")
        
        # List first 5 datasets to confirm access
        datasets = list(client.list_datasets(max_results=5))
        if datasets:
            print(f"Found {len(datasets)} datasets:")
            for dataset in datasets:
                print(f" - {dataset.dataset_id}")
        else:
            print("No datasets found in the project.")
            
    except Exception as e:
        print(f"Authentication error: {e}")
        print("Please ensure your credentials file is correct and has the right permissions.")

if __name__ == "__main__":
    main()
