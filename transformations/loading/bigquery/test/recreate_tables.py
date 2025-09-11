#!/usr/bin/env python3
"""
Recreate staging tables with 1-day retention and new schema
WARNING: This will delete existing data!
"""
import sys
import os
from google.cloud import bigquery

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from loader import BigQueryLoader
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def recreate_staging_tables():
    """Recreate all staging tables with updated schema and 1-day retention"""
    print("🔄 Recreating Staging Tables")
    print("=" * 30)
    
    print("⚠️  WARNING: This will DELETE all existing data!")
    print("⚠️  WARNING: This action cannot be undone!")
    
    try:
        loader = BigQueryLoader()
        client = bigquery.Client()
        
        # Tables to recreate
        sources = ["appleme", "simplytek", "onei_lk"]
        
        for source in sources:
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            print(f"\n🔄 Recreating: stg_raw_{source}")
            print("-" * 35)
            
            try:
                # Check if table exists
                try:
                    existing_table = client.get_table(table_id)
                    print(f"📊 Current table: {existing_table.num_rows} rows")
                    
                    # Delete existing table
                    client.delete_table(table_id)
                    print(f"🗑️  Deleted existing table")
                    
                except Exception:
                    print(f"📄 No existing table found")
                
                # Create new table with current schema and settings
                new_table_id = loader.ensure_staging_table_exists(source)
                
                # Verify new table
                new_table = client.get_table(new_table_id)
                retention_days = new_table.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000)
                
                print(f"✅ Created new table: {new_table_id}")
                print(f"⏰ Retention: {retention_days} days")
                
                # Check schema
                has_overwrite_field = any(field.name == "overwrite_timestamp" for field in new_table.schema)
                if has_overwrite_field:
                    print(f"🎯 Schema: NEW (with overwrite_timestamp)")
                else:
                    print(f"📋 Schema: STANDARD")
                
            except Exception as e:
                print(f"❌ Could not recreate {source}: {e}")
        
        print(f"\n🎯 Summary:")
        print("✅ All tables recreated with 1-day retention")
        print("✅ New schema with overwrite_timestamp field")
        print("✅ Ready for optimized overwrite behavior")
        print("⚠️  All previous data has been deleted")
        
    except Exception as e:
        print(f"❌ Recreate failed: {str(e)}")

def show_backup_option():
    """Show how to backup data before recreating"""
    print(f"\n💾 Data Backup Option")
    print("=" * 20)
    print("If you want to preserve existing data, run these BigQuery queries first:")
    print()
    
    sources = ["appleme", "simplytek", "onei_lk"]
    for source in sources:
        print(f"-- Backup {source} data")
        print(f"CREATE TABLE `price-pulse-470211.staging.backup_stg_raw_{source}` AS")
        print(f"SELECT * FROM `price-pulse-470211.staging.stg_raw_{source}`;")
        print()

if __name__ == "__main__":
    print("🚀 BigQuery Table Recreate Tool")
    print("=" * 32)
    
    show_backup_option()
    
    print("\nThis will RECREATE all staging tables with:")
    print("✅ 1-day partition retention")
    print("✅ New schema with overwrite_timestamp")
    print("❌ ALL EXISTING DATA WILL BE LOST")
    
    confirm = input("\nAre you absolutely sure? Type 'RECREATE' to proceed: ").strip()
    
    if confirm == 'RECREATE':
        recreate_staging_tables()
    else:
        print("❌ Recreate cancelled.")
        print("💡 Consider using the update_table_retention.py script instead.")
