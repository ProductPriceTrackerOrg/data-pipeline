#!/usr/bin/env python3
"""
Test the 1-day retention and overwrite behavior
"""
import sys
import os
import logging
from google.cloud import bigquery

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from loader import BigQueryLoader
    print("✅ Successfully imported BigQueryLoader")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_new_table_creation():
    """Test that newly created tables have 1-day retention"""
    print("\n🧪 Testing New Table Creation with 1-Day Retention")
    print("=" * 55)
    
    try:
        loader = BigQueryLoader()
        client = bigquery.Client()
        
        # Test creating a new table for a test source
        test_source = "test_source_" + str(int(time.time()))[-6:]  # Unique name
        print(f"🔧 Creating test table for source: {test_source}")
        
        # This will create a new table with current settings
        table_id = loader.ensure_staging_table_exists(test_source)
        print(f"✅ Table created: {table_id}")
        
        # Get the table and check its settings
        table = client.get_table(table_id)
        
        if table.time_partitioning and table.time_partitioning.expiration_ms:
            expiration_days = table.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000)
            print(f"⏰ Partition expiration: {expiration_days} days")
            
            if expiration_days == 1:
                print("🎯 SUCCESS: New table has 1-day retention!")
            else:
                print(f"⚠️  Expected 1 day, got {expiration_days} days")
        else:
            print("⚠️  No partition expiration set")
            
        # Clean up - delete the test table
        try:
            client.delete_table(table_id)
            print(f"🗑️  Cleaned up test table: {test_source}")
        except Exception as e:
            print(f"⚠️  Could not clean up test table: {e}")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def test_overwrite_behavior():
    """Test the overwrite behavior"""
    print("\n🧪 Testing Data Overwrite Behavior")
    print("=" * 40)
    
    try:
        loader = BigQueryLoader()
        client = bigquery.Client()
        
        # Use existing appleme table for testing
        test_source = "appleme"
        test_date = "2025-09-06"
        
        # Count existing rows
        table_id = f"price-pulse-470211.staging.stg_raw_{test_source}"
        count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}` WHERE source_website = '{test_source}'"
        
        result = client.query(count_query).result()
        initial_count = list(result)[0].row_count
        print(f"📊 Initial rows for {test_source}: {initial_count}")
        
        # Load some test data
        test_products = [
            {"product_title": "Test Product 1", "price": "1000"},
            {"product_title": "Test Product 2", "price": "2000"}
        ]
        
        print(f"🔄 Loading {len(test_products)} test products...")
        loaded_count = loader.load_source_data(
            source=test_source,
            products=test_products,
            scrape_date=test_date,
            file_path="test/overwrite_test.json"
        )
        
        if loaded_count > 0:
            print(f"✅ Loaded {loaded_count} products")
            
            # Check final count
            result = client.query(count_query).result()
            final_count = list(result)[0].row_count
            print(f"📊 Final rows for {test_source}: {final_count}")
            
            if final_count == 1:
                print("🎯 SUCCESS: Data was overwritten (only 1 row exists)")
            else:
                print(f"⚠️  Expected 1 row, found {final_count} rows")
                
        else:
            print("❌ Failed to load test data")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import time
    
    print("🚀 BigQuery Loader Tests")
    print("=" * 25)
    
    test_new_table_creation()
    test_overwrite_behavior()
    
    print("\n✅ All tests completed!")
