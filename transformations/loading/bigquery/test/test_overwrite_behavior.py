#!/usr/bin/env python3
"""
Test the new 1-day retention and overwrite behavior
"""
import sys
import os
import logging
from google.cloud import bigquery

# Add the parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loader import BigQueryLoader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_overwrite_behavior():
    """Test that new data overwrites old data immediately"""
    print("\n🧪 Testing New Overwrite Behavior")
    print("=" * 50)
    
    loader = BigQueryLoader()
    client = bigquery.Client()
    
    # Test data
    test_source = "appleme"
    test_scrape_date = "2025-09-06"
    
    # First load - some test data
    test_products_1 = [
        {"product_title": "Test Product 1", "price": "1000"},
        {"product_title": "Test Product 2", "price": "2000"}
    ]
    
    print(f"\n1️⃣ First Load: {len(test_products_1)} products")
    count1 = loader.load_source_data(
        source=test_source,
        products=test_products_1,
        scrape_date=test_scrape_date,
        file_path="test/first_load.json"
    )
    print(f"✅ Loaded: {count1} products")
    
    # Check data count
    table_id = f"price-pulse-470211.staging.stg_raw_{test_source}"
    count_query = f"SELECT COUNT(*) as row_count FROM `{table_id}` WHERE source_website = '{test_source}'"
    result = client.query(count_query).result()
    row_count_after_first = list(result)[0].row_count
    print(f"📊 Rows in table after first load: {row_count_after_first}")
    
    # Second load - different data (should overwrite)
    test_products_2 = [
        {"product_title": "NEW Test Product A", "price": "5000"},
        {"product_title": "NEW Test Product B", "price": "6000"},
        {"product_title": "NEW Test Product C", "price": "7000"}
    ]
    
    print(f"\n2️⃣ Second Load: {len(test_products_2)} products (should overwrite)")
    count2 = loader.load_source_data(
        source=test_source,
        products=test_products_2,
        scrape_date=test_scrape_date,
        file_path="test/second_load.json"
    )
    print(f"✅ Loaded: {count2} products")
    
    # Check data count again
    result = client.query(count_query).result()
    row_count_after_second = list(result)[0].row_count
    print(f"📊 Rows in table after second load: {row_count_after_second}")
    
    # Verify overwrite behavior
    if row_count_after_second == 1:
        print("🎯 SUCCESS: Data was overwritten (only 1 row exists)")
        
        # Check actual data
        data_query = f"""
        SELECT 
            product_count,
            file_path,
            loaded_at
        FROM `{table_id}` 
        WHERE source_website = '{test_source}'
        ORDER BY loaded_at DESC
        """
        data_result = client.query(data_query).result()
        for row in data_result:
            print(f"📄 Current data: {row.product_count} products from {row.file_path}")
            
    else:
        print(f"❌ FAILED: Expected 1 row, got {row_count_after_second} rows")
    
    # Check partition retention setting
    retention_query = f"""
    SELECT 
        table_name,
        ROUND(time_partitioning_expiration_ms / (24 * 60 * 60 * 1000)) as retention_days
    FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = 'stg_raw_{test_source}'
    """
    
    retention_result = client.query(retention_query).result()
    for row in retention_result:
        print(f"⏰ Partition retention: {row.retention_days} days")
        if row.retention_days == 1:
            print("🎯 SUCCESS: 1-day retention confirmed")
        else:
            print(f"❌ FAILED: Expected 1 day, got {row.retention_days} days")

if __name__ == "__main__":
    test_overwrite_behavior()
