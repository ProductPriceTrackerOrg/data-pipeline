#!/usr/bin/env python3
"""
Step-by-step permission verification
"""
import time
import os
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

def test_permissions_step_by_step():
    """Test permissions with detailed feedback"""
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'price-pulse-470211')
    
    print(f"🔄 Step-by-Step Permission Test")
    print(f"=" * 50)
    
    try:
        print(f"1️⃣ Creating BigQuery client...")
        client = bigquery.Client(project=project_id)
        print(f"   ✅ Client created successfully")
        print(f"   📋 Using project: {client.project}")
        
        print(f"\n2️⃣ Testing basic query execution...")
        try:
            # Very simple query that requires minimal permissions
            query = "SELECT 1 as test_value"
            job_config = bigquery.QueryJobConfig(dry_run=False, use_query_cache=False)
            
            print(f"   🔄 Running test query...")
            query_job = client.query(query, job_config=job_config)
            results = query_job.result()
            
            for row in results:
                print(f"   ✅ Query executed successfully: {row.test_value}")
            
            print(f"\n3️⃣ Testing dataset listing...")
            datasets = list(client.list_datasets())
            print(f"   ✅ Found {len(datasets)} datasets")
            
            if datasets:
                for dataset in datasets:
                    print(f"      - {dataset.dataset_id}")
            else:
                print(f"      (No datasets found - this is normal)")
            
            print(f"\n4️⃣ Testing dataset creation permissions...")
            try:
                # Try to create a temporary dataset
                test_dataset_id = f"{project_id}.temp_permission_test"
                test_dataset = bigquery.Dataset(test_dataset_id)
                test_dataset.location = "US"
                test_dataset.description = "Temporary dataset for permission testing"
                
                created_dataset = client.create_dataset(test_dataset, exists_ok=True)
                print(f"   ✅ Dataset creation successful!")
                
                # Clean up
                client.delete_dataset(test_dataset_id, delete_contents=True)
                print(f"   ✅ Test dataset cleaned up")
                
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"   ✅ Dataset creation permissions OK (dataset exists)")
                else:
                    print(f"   ❌ Dataset creation failed: {e}")
                    return False
            
            print(f"\n🎉 ALL PERMISSIONS ARE WORKING!")
            return True
            
        except Exception as e:
            if "permission" in str(e).lower() or "access denied" in str(e).lower():
                print(f"   ❌ Permission denied: {e}")
                print(f"\n💡 SOLUTION:")
                print(f"   1. Go to: https://console.cloud.google.com/iam-admin/iam")
                print(f"   2. Find: bigquery-loader@price-pulse-470211.iam.gserviceaccount.com")
                print(f"   3. Edit and add role: 'BigQuery Admin'")
                print(f"   4. Wait 2-3 minutes for propagation")
                print(f"   5. Try again")
                return False
            else:
                print(f"   ❌ Unexpected error: {e}")
                return False
                
    except Exception as e:
        print(f"❌ Failed to create BigQuery client: {e}")
        return False

if __name__ == "__main__":
    success = test_permissions_step_by_step()
    
    if not success:
        print(f"\n⏰ If you just added the permissions, wait 2-3 minutes and try again:")
        print(f"   python {__file__.split('/')[-1]}")
    else:
        print(f"\n🚀 Ready to test your 7-day partition expiration!")
        print(f"   python test_table_creation.py")
