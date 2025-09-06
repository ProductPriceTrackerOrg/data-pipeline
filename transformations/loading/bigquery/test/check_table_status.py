#!/usr/bin/env python3
"""
Check the current state of staging tables and their schemas
"""
import sys
import os
from google.cloud import bigquery

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def check_table_schemas():
    """Check the schemas of all staging tables"""
    print("🔍 Staging Table Schema Analysis")
    print("=" * 35)
    
    try:
        client = bigquery.Client()
        
        # Check all known staging tables
        sources = ["appleme", "simplytek", "onei_lk"]
        
        for source in sources:
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            print(f"\n📊 Table: stg_raw_{source}")
            print("-" * 30)
            
            try:
                table = client.get_table(table_id)
                
                # Check basic info
                print(f"🕐 Created: {table.created}")
                print(f"📝 Rows: {table.num_rows:,}")
                
                # Check partition settings
                if table.time_partitioning:
                    expiration_days = table.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000) if table.time_partitioning.expiration_ms else "None"
                    print(f"📅 Partitioning: {table.time_partitioning.field} (expires: {expiration_days} days)")
                
                # Check schema
                print("📋 Schema fields:")
                has_overwrite_field = False
                for field in table.schema:
                    if field.name == "overwrite_timestamp":
                        has_overwrite_field = True
                        print(f"  ✅ {field.name} ({field.field_type}) - NEW FIELD")
                    else:
                        print(f"  📄 {field.name} ({field.field_type})")
                
                # Schema status
                if has_overwrite_field:
                    print("🎯 Status: NEW SCHEMA (supports improved overwrite)")
                else:
                    print("📋 Status: LEGACY SCHEMA (basic overwrite only)")
                
                # Check current data
                count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}` WHERE source_website = '{source}'"
                result = client.query(count_query).result()
                row_count = list(result)[0].row_count
                
                if row_count == 1:
                    print(f"📊 Data: CLEAN ({row_count} row)")
                else:
                    print(f"📊 Data: MULTIPLE ROWS ({row_count} rows)")
                    
            except Exception as e:
                print(f"❌ Could not access table: {e}")
        
        print(f"\n🎯 Summary:")
        print("- NEW tables get 1-day retention + improved overwrite")
        print("- LEGACY tables work with basic overwrite (rely on 1-day retention)")
        print("- All tables benefit from cost savings")
        
    except Exception as e:
        print(f"❌ Analysis failed: {str(e)}")

def show_partition_settings():
    """Show partition settings for all tables"""
    print(f"\n📅 Partition Settings Check")
    print("=" * 30)
    
    try:
        client = bigquery.Client()
        
        # Query to get partition info (using DDL method that works)
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
                    print(f"  ⏰ {line.strip()}")
                elif 'partition by' in line.lower():
                    print(f"  📅 {line.strip()}")
                    
    except Exception as e:
        print(f"❌ Partition check failed: {str(e)}")

if __name__ == "__main__":
    check_table_schemas()
    show_partition_settings()
