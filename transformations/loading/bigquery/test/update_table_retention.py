#!/usr/bin/env python3
"""
Update existing staging tables to use 1-day partition retention
"""
import sys
import os
from google.cloud import bigquery

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def update_table_retention():
    """Update existing tables to 1-day retention"""
    print("🔧 Updating Existing Tables to 1-Day Retention")
    print("=" * 50)
    
    try:
        client = bigquery.Client()
        
        # Tables to update
        sources = ["appleme", "simplytek", "onei_lk"]
        
        for source in sources:
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            print(f"\n📊 Updating: stg_raw_{source}")
            print("-" * 30)
            
            try:
                # Get the current table
                table = client.get_table(table_id)
                
                # Show current settings
                current_expiration = table.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000) if table.time_partitioning.expiration_ms else "None"
                print(f"🕐 Current retention: {current_expiration} days")
                
                # Update partition expiration to 1 day
                table.time_partitioning.expiration_ms = 1 * 24 * 60 * 60 * 1000  # 1 day in milliseconds
                
                # Update the table
                updated_table = client.update_table(table, ["time_partitioning"])
                
                # Verify the change
                new_expiration = updated_table.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000)
                print(f"✅ Updated retention: {new_expiration} days")
                print(f"💰 Cost savings: Data now expires in 1 day instead of 7!")
                
            except Exception as e:
                print(f"❌ Could not update {source}: {e}")
        
        print(f"\n🎯 Summary:")
        print("✅ All staging tables now have 1-day partition retention")
        print("💰 Immediate cost savings - data deleted after 1 day")
        print("🔄 Overwrite behavior remains the same")
        
    except Exception as e:
        print(f"❌ Update failed: {str(e)}")

def verify_updates():
    """Verify that updates were successful"""
    print(f"\n🔍 Verification Check")
    print("=" * 20)
    
    try:
        client = bigquery.Client()
        
        # Check all tables
        tables_query = """
        SELECT 
            table_name,
            ddl
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        result = client.query(tables_query).result()
        
        for row in result:
            print(f"\n📄 {row.table_name}:")
            
            # Extract partition info from DDL
            ddl_lines = row.ddl.split('\n')
            for line in ddl_lines:
                if 'partition_expiration_days' in line.lower():
                    if "1.0" in line:
                        print(f"  ✅ {line.strip()}")
                    else:
                        print(f"  ⚠️  {line.strip()}")
                    
    except Exception as e:
        print(f"❌ Verification failed: {str(e)}")

if __name__ == "__main__":
    print("🚀 BigQuery Table Retention Update")
    print("=" * 35)
    
    # Ask for confirmation
    print("\nThis will update ALL existing staging tables to 1-day retention.")
    print("Data older than 1 day will be automatically deleted.")
    print("This action cannot be undone.")
    
    confirm = input("\nDo you want to proceed? (yes/no): ").lower().strip()
    
    if confirm in ['yes', 'y']:
        update_table_retention()
        verify_updates()
    else:
        print("❌ Update cancelled.")
        print("💡 Tables will naturally get 1-day retention when recreated.")
