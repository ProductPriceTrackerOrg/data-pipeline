#!/usr/bin/env python3
"""
Clear all data from staging tables for a fresh start
"""
import sys
import os
from google.cloud import bigquery

def clear_staging_data():
    """Clear all data from staging tables"""
    print("🗑️  Clearing All Staging Data")
    print("=" * 30)
    
    try:
        client = bigquery.Client()
        
        # Tables to clear
        sources = ["appleme", "simplytek", "onei_lk"]
        
        for source in sources:
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            print(f"\n📊 Clearing: stg_raw_{source}")
            print("-" * 25)
            
            try:
                # Check current row count
                count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
                result = client.query(count_query).result()
                current_rows = list(result)[0].row_count
                print(f"📄 Current rows: {current_rows}")
                
                if current_rows > 0:
                    # Delete all data
                    delete_query = f"DELETE FROM `{table_id}` WHERE TRUE"
                    delete_job = client.query(delete_query)
                    delete_job.result()  # Wait for completion
                    
                    print(f"✅ Deleted {current_rows} rows")
                else:
                    print(f"📭 Table already empty")
                
            except Exception as e:
                print(f"❌ Could not clear {source}: {e}")
        
        print(f"\n🎯 Summary:")
        print("✅ All staging tables cleared")
        print("✅ Tables still exist with 1-day retention")
        print("✅ Ready for fresh data loads")
        print("🔄 Your next scrape will populate clean tables")
        
    except Exception as e:
        print(f"❌ Clear failed: {str(e)}")

def verify_clear():
    """Verify that all tables are empty"""
    print(f"\n🔍 Verification Check")
    print("=" * 20)
    
    try:
        client = bigquery.Client()
        
        sources = ["appleme", "simplytek", "onei_lk"]
        total_rows = 0
        
        for source in sources:
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            try:
                count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}`"
                result = client.query(count_query).result()
                row_count = list(result)[0].row_count
                total_rows += row_count
                
                if row_count == 0:
                    print(f"✅ stg_raw_{source}: EMPTY")
                else:
                    print(f"⚠️  stg_raw_{source}: {row_count} rows remaining")
                    
            except Exception as e:
                print(f"❌ Could not check {source}: {e}")
        
        if total_rows == 0:
            print(f"\n🎯 SUCCESS: All staging tables are empty!")
            print("Ready for fresh start! 🚀")
        else:
            print(f"\n⚠️  {total_rows} total rows remaining")
            
    except Exception as e:
        print(f"❌ Verification failed: {str(e)}")

if __name__ == "__main__":
    print("🚀 BigQuery Staging Data Clear")
    print("=" * 30)
    
    print("\nThis will DELETE ALL DATA from staging tables.")
    print("Tables will remain with 1-day retention settings.")
    print("This action cannot be undone.")
    
    confirm = input("\nDo you want to proceed? (yes/no): ").lower().strip()
    
    if confirm in ['yes', 'y']:
        clear_staging_data()
        verify_clear()
    else:
        print("❌ Clear cancelled.")
        print("💡 Data remains in staging tables.")
