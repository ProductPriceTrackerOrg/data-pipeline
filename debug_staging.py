#!/usr/bin/env python3
"""
Debug script to check staging data structure for 2025-09-06
"""

from google.cloud import bigquery
import json

client = bigquery.Client()

# Check data availability
print("🔍 Checking data availability for 2025-09-06:")

query = """
SELECT 
    COUNT(*) as record_count,
    scrape_date,
    source_website
FROM `price-pulse-470211.staging.stg_raw_appleme`
WHERE scrape_date = '2025-09-06'
GROUP BY scrape_date, source_website
"""

results = list(client.query(query).result())
for row in results:
    print(f"  {row.source_website}: {row.record_count} records on {row.scrape_date}")

if results:
    # Get a sample record to examine structure
    print("\n📋 Examining data structure:")
    
    sample_query = """
    SELECT 
        scrape_date,
        source_website,
        raw_json_data
    FROM `price-pulse-470211.staging.stg_raw_appleme`
    WHERE scrape_date = '2025-09-06'
    LIMIT 1
    """
    
    sample_results = list(client.query(sample_query).result())
    if sample_results:
        row = sample_results[0]
        print(f"Source: {row.source_website}")
        print(f"Date: {row.scrape_date}")
        
        try:
            data = json.loads(row.raw_json_data)
            print(f"JSON type: {type(data)}")
            
            if isinstance(data, list):
                print(f"Array length: {len(data)}")
                if len(data) > 0:
                    first_product = data[0]
                    print(f"First product keys: {list(first_product.keys())}")
                    if 'variants' in first_product:
                        variants = first_product['variants']
                        print(f"Variants: {type(variants)}, length: {len(variants)}")
                        if len(variants) > 0:
                            print(f"First variant: {variants[0]}")
            else:
                print(f"Not an array! Type: {type(data)}")
                
        except Exception as e:
            print(f"Error parsing JSON: {e}")
            print(f"Raw data preview: {row.raw_json_data[:200]}...")
else:
    print("❌ No data found for 2025-09-06!")
