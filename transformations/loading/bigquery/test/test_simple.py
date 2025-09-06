#!/usr/bin/env python3
"""
Simple test to verify the BigQuery loader class and 1-day retention setting
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

def test_simple():
    """Simple test to verify the loader works"""
    print("\n🧪 Simple BigQuery Loader Test")
    print("=" * 40)
    
    try:
        # Test creating loader instance
        loader = BigQueryLoader()
        print("✅ BigQueryLoader instance created")
        
        # Test BigQuery client
        client = bigquery.Client()
        print("✅ BigQuery client connected")
        
        # Check partition retention setting in existing tables
        retention_query = """
        SELECT 
            table_name,
            ddl
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        """
        
        print("\n📊 Checking partition retention settings...")
        result = client.query(retention_query).result()
        
        for row in result:
            print(f"📄 Table: {row.table_name}")
            
            # Check DDL for partition expiration
            if "partition_expiration_days=1" in row.ddl.lower():
                print("🎯 SUCCESS: 1-day retention found in DDL!")
            elif "partition_expiration_days" in row.ddl.lower():
                print(f"⚠️  Note: Table has different retention setting")
            else:
                print("📋 Partition settings not found in DDL")
                
            # Show relevant part of DDL
            ddl_lines = row.ddl.split('\n')
            for line in ddl_lines:
                if 'partition' in line.lower():
                    print(f"📋 DDL: {line.strip()}")
        
        print("\n✅ Basic test completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple()
