#!/usr/bin/env python3
"""
Test BigQuery with default credentials (from gcloud auth)
"""
import os
import sys
from pathlib import Path
from google.cloud import bigquery

def test_with_default_credentials():
    """Test BigQuery with default credentials"""
    project_id = "price-pulse-470211"
    
    print(f"🔄 Testing with Default Credentials (gcloud auth)")
    print(f"=" * 50)
    
    # Temporarily clear the service account credentials
    old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if old_creds:
        print(f"📋 Temporarily removing service account credentials")
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    try:
        print(f"1️⃣ Creating BigQuery client with default credentials...")
        client = bigquery.Client(project=project_id)
        print(f"   ✅ Client created successfully")
        print(f"   📋 Using project: {client.project}")
        
        print(f"\n2️⃣ Testing basic query execution...")
        query = "SELECT 'Hello BigQuery!' as message, CURRENT_TIMESTAMP() as current_time"
        
        query_job = client.query(query)
        results = query_job.result()
        
        for row in results:
            print(f"   ✅ Query successful: {row.message}")
            print(f"   🕐 Time: {row.current_time}")
        
        print(f"\n3️⃣ Testing dataset listing...")
        datasets = list(client.list_datasets())
        print(f"   ✅ Found {len(datasets)} datasets")
        
        for dataset in datasets:
            print(f"      - {dataset.dataset_id}")
        
        print(f"\n4️⃣ Testing dataset creation...")
        try:
            dataset_id = f"{project_id}.staging"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            dataset.description = "Staging dataset for raw scraped data"
            
            # Try to create or get existing
            try:
                created_dataset = client.create_dataset(dataset, timeout=30)
                print(f"   ✅ Created new dataset: {dataset_id}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    existing_dataset = client.get_dataset(dataset_id)
                    print(f"   ✅ Dataset already exists: {dataset_id}")
                else:
                    raise e
            
            print(f"\n🎉 SUCCESS! BigQuery is working with gcloud credentials!")
            return True
            
        except Exception as e:
            print(f"   ❌ Dataset creation failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    finally:
        # Restore the original credentials
        if old_creds:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = old_creds
            print(f"\n📋 Restored service account credentials")

if __name__ == "__main__":
    success = test_with_default_credentials()
    
    if success:
        print(f"\n🚀 Great! Now let's test your 7-day partition expiration:")
        print(f"   We can modify the loader to use default credentials instead")
    else:
        print(f"\n❌ Still having issues. Let's debug further.")
