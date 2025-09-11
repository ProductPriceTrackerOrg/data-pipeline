#!/usr/bin/env python3
"""
Create BigQuery dataset manually
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

def create_dataset():
    """Create the staging dataset"""
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT', 'price-pulse-470211')
    dataset_name = 'staging'
    
    print(f"🏗️  Creating dataset {project_id}.{dataset_name}")
    
    try:
        client = bigquery.Client(project=project_id)
        
        # Check if dataset exists
        try:
            dataset_ref = client.get_dataset(dataset_name)
            print(f"✅ Dataset {project_id}.{dataset_name} already exists")
            return True
        except Exception:
            pass
        
        # Create dataset
        dataset_id = f"{project_id}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset.description = "Staging dataset for raw scraped data from ADLS"
        
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"🎉 Successfully created dataset {dataset_id}")
        return True
        
    except Exception as e:
        if "Access Denied" in str(e) or "does not have bigquery.datasets.create permission" in str(e):
            print(f"❌ Permission denied. You need BigQuery Admin role or bigquery.datasets.create permission")
            print(f"💡 Ask your admin to create the dataset with this command:")
            print(f"   CREATE SCHEMA `{project_id}.{dataset_name}`")
            print(f"   OR grant your service account the 'BigQuery Admin' role")
            return False
        else:
            print(f"❌ Error creating dataset: {e}")
            return False

if __name__ == "__main__":
    create_dataset()
