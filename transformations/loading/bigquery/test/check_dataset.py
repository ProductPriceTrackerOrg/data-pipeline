#!/usr/bin/env python3
"""
Check if BigQuery dataset exists
"""
import os
import sys
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

def check_dataset():
    """Check if the staging dataset exists"""
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'price-pulse-470211')
    dataset_name = 'staging'
    
    print(f"🔍 Checking for dataset: {project_id}.{dataset_name}")
    
    try:
        client = bigquery.Client(project=project_id)
        
        # List all datasets
        datasets = list(client.list_datasets())
        print(f"📋 Found {len(datasets)} datasets in project:")
        
        for dataset in datasets:
            print(f"   - {dataset.dataset_id}")
            if dataset.dataset_id == dataset_name:
                print(f"✅ Target dataset '{dataset_name}' found!")
                
                # Get dataset details
                dataset_ref = client.get_dataset(dataset_name)
                print(f"   Location: {dataset_ref.location}")
                print(f"   Description: {dataset_ref.description}")
                print(f"   Created: {dataset_ref.created}")
                return True
        
        print(f"❌ Dataset '{dataset_name}' not found")
        print(f"💡 Please create it in BigQuery Console")
        return False
        
    except Exception as e:
        print(f"❌ Error checking datasets: {e}")
        return False

if __name__ == "__main__":
    check_dataset()
