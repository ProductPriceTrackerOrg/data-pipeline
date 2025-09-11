#!/usr/bin/env python3
"""
Debug BigQuery connection and project access
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

def debug_connection():
    """Debug BigQuery connection"""
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'price-pulse-470211')
    
    print(f"🔍 Debugging BigQuery Connection")
    print(f"📋 Environment Variables:")
    print(f"   GOOGLE_CLOUD_PROJECT: {project_id}")
    print(f"   GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
    
    try:
        client = bigquery.Client(project=project_id)
        
        print(f"\n🔗 BigQuery Client Info:")
        print(f"   Project: {client.project}")
        print(f"   Location: {client.location}")
        
        # Test basic access
        print(f"\n🧪 Testing basic access...")
        
        # Try to run a simple query
        query = f"SELECT '{project_id}' as project_id, CURRENT_TIMESTAMP() as current_time"
        results = client.query(query)
        
        for row in results:
            print(f"✅ Connected to project: {row.project_id}")
            print(f"✅ Current time: {row.current_time}")
        
        # List datasets with more details
        print(f"\n📊 Listing all datasets...")
        datasets = list(client.list_datasets())
        
        if datasets:
            print(f"Found {len(datasets)} datasets:")
            for dataset in datasets:
                full_id = f"{client.project}.{dataset.dataset_id}"
                print(f"   - {full_id}")
                
                # Try to get dataset details
                try:
                    dataset_ref = client.get_dataset(dataset.dataset_id)
                    print(f"     Location: {dataset_ref.location}")
                    print(f"     Created: {dataset_ref.created}")
                except Exception as e:
                    print(f"     Error getting details: {e}")
        else:
            print("No datasets found")
            
        # Check if we have permission to create datasets
        print(f"\n🔐 Testing dataset creation permissions...")
        try:
            # Try to create a test dataset
            test_dataset_id = f"{project_id}.test_permissions_check"
            test_dataset = bigquery.Dataset(test_dataset_id)
            test_dataset.location = "US"
            
            # This will fail if we don't have permissions
            created_dataset = client.create_dataset(test_dataset, exists_ok=False)
            print(f"✅ Can create datasets")
            
            # Clean up
            client.delete_dataset(test_dataset_id, delete_contents=True)
            print(f"✅ Test dataset cleaned up")
            
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"✅ Dataset creation permissions OK (test dataset already exists)")
            elif "permission" in str(e).lower() or "access denied" in str(e).lower():
                print(f"❌ No permission to create datasets: {e}")
            else:
                print(f"⚠️ Unknown error testing permissions: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to BigQuery: {e}")
        return False

if __name__ == "__main__":
    debug_connection()
