#!/usr/bin/env python3
"""
Test table creation with 7-day partition expiration (assumes dataset exists)
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

def test_table_creation():
    """Test table creation with partition expiration"""
    project_id = "price-pulse-470211"  # Use hardcoded project
    dataset_name = 'staging'
    
    print(f"🧪 Testing table creation with 7-day partition expiration")
    
    # Use default credentials instead of service account
    old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if old_creds:
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    try:
        client = bigquery.Client(project=project_id)
        
        # Check if dataset exists
        try:
            dataset_ref = client.get_dataset(dataset_name)
            print(f"✅ Dataset {project_id}.{dataset_name} exists")
        except Exception as e:
            print(f"❌ Dataset {project_id}.{dataset_name} does not exist")
            print(f"💡 Please create it first using BigQuery Console")
            return False
        
        # Test table creation
        table_name = "test_source_staging"
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        
        # Define schema
        schema = [
            bigquery.SchemaField("source_website", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scrape_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("scrape_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("product_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("raw_json_data", "JSON", mode="REQUIRED"),
        ]
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        
        # Set up partitioning
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="scrape_date"
        )
        
        # Set clustering
        table.clustering_fields = ["source_website"]
        
        # Set description
        table.description = "Test table for partition expiration"
        
        # ⭐ YOUR MODIFICATION: Set partition expiration (7 days / 1 week)
        table.time_partitioning.expiration_ms = 7 * 24 * 60 * 60 * 1000
        
        # Create table
        table = client.create_table(table, exists_ok=True)
        
        print(f"✅ Created table {table_name}")
        print(f"📅 Partition expiration: 7 days ({table.time_partitioning.expiration_ms:,} ms)")
        print(f"🎯 Partitioning field: {table.time_partitioning.field}")
        print(f"🏷️  Clustering fields: {table.clustering_fields}")
        
        # Cleanup - delete test table
        client.delete_table(table_id)
        print(f"🧹 Cleaned up test table")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_table_creation()
    if success:
        print(f"\n🎉 SUCCESS: Your 7-day partition expiration is working perfectly!")
    else:
        print(f"\n❌ Please create the dataset first in BigQuery Console")
