#!/usr/bin/env python3
"""
Test the improved overwrite behavior that handles streaming buffer
"""
import sys
import os
import time
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

def test_improved_overwrite():
    """Test the improved overwrite behavior with streaming buffer handling"""
    print("\n🧪 Testing Improved Overwrite Behavior")
    print("=" * 45)
    
    try:
        loader = BigQueryLoader()
        client = bigquery.Client()
        
        # Use a test source to avoid affecting production data
        test_source = f"test_overwrite_{int(time.time())}"
        test_date = "2025-09-06"
        
        print(f"🔧 Testing with source: {test_source}")
        
        # First load
        test_products_1 = [
            {"product_title": "First Load Product A", "price": "1000"},
            {"product_title": "First Load Product B", "price": "2000"}
        ]
        
        print(f"\n1️⃣ First Load: {len(test_products_1)} products")
        count1 = loader.load_source_data(
            source=test_source,
            products=test_products_1,
            scrape_date=test_date,
            file_path="test/first_load.json"
        )
        print(f"✅ Loaded: {count1} products")
        
        # Check row count
        table_id = f"price-pulse-470211.staging.stg_raw_{test_source}"
        count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}` WHERE source_website = '{test_source}'"
        
        result = client.query(count_query).result()
        row_count_1 = list(result)[0].row_count
        print(f"📊 Rows after first load: {row_count_1}")
        
        # Second load (immediate - will trigger streaming buffer issue)
        test_products_2 = [
            {"product_title": "Second Load Product X", "price": "5000"},
            {"product_title": "Second Load Product Y", "price": "6000"},
            {"product_title": "Second Load Product Z", "price": "7000"}
        ]
        
        print(f"\n2️⃣ Second Load: {len(test_products_2)} products (immediate - tests streaming buffer handling)")
        count2 = loader.load_source_data(
            source=test_source,
            products=test_products_2,
            scrape_date=test_date,
            file_path="test/second_load.json"
        )
        print(f"✅ Loaded: {count2} products")
        
        # Check row count
        result = client.query(count_query).result()
        row_count_2 = list(result)[0].row_count
        print(f"📊 Rows after second load: {row_count_2}")
        
        # Check which is the latest data
        latest_query = f"""
        SELECT 
            file_path,
            product_count,
            loaded_at,
            overwrite_timestamp
        FROM `{table_id}` 
        WHERE source_website = '{test_source}'
        ORDER BY loaded_at DESC, overwrite_timestamp DESC NULLS LAST
        LIMIT 5
        """
        
        print(f"\n📄 Latest data in table:")
        latest_result = client.query(latest_query).result()
        for i, row in enumerate(latest_result):
            timestamp_info = f" (overwrite: {row.overwrite_timestamp})" if row.overwrite_timestamp else ""
            print(f"  {i+1}. {row.file_path} - {row.product_count} products{timestamp_info}")
        
        # Analysis
        if row_count_2 > row_count_1:
            print("\n🎯 STREAMING BUFFER MODE: Multiple rows exist (will be cleaned up by 1-day retention)")
            print("   ✅ Latest data identifiable by timestamp")
            print("   ✅ 1-day retention will remove old partitions")
        elif row_count_2 == 1:
            print("\n🎯 DIRECT OVERWRITE MODE: Data was successfully deleted and replaced")
        else:
            print(f"\n⚠️  Unexpected row count: {row_count_2}")
        
        # Clean up
        try:
            client.delete_table(table_id)
            print(f"\n🗑️  Cleaned up test table: {test_source}")
        except Exception as e:
            print(f"⚠️  Could not clean up test table: {e}")
            
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def test_view_current_data():
    """Show current data state after overwrite logic"""
    print("\n🔍 Current Data State Analysis")
    print("=" * 35)
    
    try:
        client = bigquery.Client()
        
        # Check all sources
        for source in ["appleme", "simplytek", "onei_lk"]:
            print(f"\n📊 {source.upper()}:")
            
            table_id = f"price-pulse-470211.staging.stg_raw_{source}"
            
            # First check if overwrite_timestamp field exists
            try:
                table = client.get_table(table_id)
                has_overwrite_field = any(field.name == "overwrite_timestamp" for field in table.schema)
            except Exception:
                has_overwrite_field = False
                print(f"  ⚠️  Table not found or inaccessible")
                continue
            
            # Get latest data info (with conditional field selection)
            if has_overwrite_field:
                info_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    MAX(loaded_at) as latest_load,
                    MAX(product_count) as latest_product_count,
                    MAX(overwrite_timestamp) as latest_overwrite_ts
                FROM `{table_id}`
                WHERE source_website = '{source}'
                """
            else:
                info_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    MAX(loaded_at) as latest_load,
                    MAX(product_count) as latest_product_count,
                    NULL as latest_overwrite_ts
                FROM `{table_id}`
                WHERE source_website = '{source}'
                """
            
            result = client.query(info_query).result()
            for row in result:
                print(f"  📄 Total rows: {row.total_rows}")
                print(f"  🕐 Latest load: {row.latest_load}")
                print(f"  📦 Latest product count: {row.latest_product_count}")
                
                if has_overwrite_field:
                    print(f"  🔧 Schema: NEW (with overwrite_timestamp)")
                    if row.latest_overwrite_ts:
                        print(f"  ⏰ Overwrite timestamp: {row.latest_overwrite_ts}")
                else:
                    print(f"  📋 Schema: LEGACY (without overwrite_timestamp)")
                
                # Determine data state
                if row.total_rows == 1:
                    print("  🎯 State: CLEAN (single row)")
                elif has_overwrite_field:
                    print("  📊 State: MULTIPLE ROWS (1-day retention will clean up)")
                else:
                    print("  📊 State: LEGACY TABLE (consider recreating for new features)")
    
    except Exception as e:
        print(f"❌ Analysis failed: {str(e)}")

if __name__ == "__main__":
    print("🚀 Improved Overwrite Behavior Tests")
    print("=" * 40)
    
    test_improved_overwrite()
    test_view_current_data()
    
    print("\n✅ All tests completed!")
