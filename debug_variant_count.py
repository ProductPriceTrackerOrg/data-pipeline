#!/usr/bin/env python3
"""
Debug script to check variant counts in staging data
"""

from google.cloud import bigquery
import json

def main():
    client = bigquery.Client()
    
    print("=== Checking staging data for 2025-09-06 ===")
    
    # Check appleme staging
    query = """
    SELECT 
        scraped_date,
        ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_json_data)) as product_count
    FROM `price-pulse-470211.staging.appleme_staging` 
    WHERE scraped_date = '2025-09-06'
    """
    
    result = list(client.query(query).result())
    if result:
        row = result[0]
        print(f"appleme: {row.product_count} products")
    else:
        print("appleme: No data found")
    
    # Check simplytek staging
    query = """
    SELECT 
        scraped_date,
        ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_json_data)) as product_count
    FROM `price-pulse-470211.staging.simplytek_staging` 
    WHERE scraped_date = '2025-09-06'
    """
    
    result = list(client.query(query).result())
    if result:
        row = result[0]
        print(f"simplytek: {row.product_count} products")
    else:
        print("simplytek: No data found")
    
    # Now check variant counts
    print("\n=== Checking variant counts ===")
    
    query = """
    WITH appleme_variants AS (
      SELECT 
        JSON_VALUE(product, '$.product_id_native') as product_id_native,
        variant
      FROM `price-pulse-470211.staging.appleme_staging`,
      UNNEST(JSON_QUERY_ARRAY(raw_json_data)) AS product,
      UNNEST(JSON_QUERY_ARRAY(product, '$.variants')) AS variant
      WHERE scraped_date = '2025-09-06'
    )
    SELECT COUNT(*) as variant_count
    FROM appleme_variants
    """
    
    result = list(client.query(query).result())[0]
    print(f"appleme variants: {result.variant_count}")
    
    query = """
    WITH simplytek_variants AS (
      SELECT 
        JSON_VALUE(product, '$.product_id_native') as product_id_native,
        variant
      FROM `price-pulse-470211.staging.simplytek_staging`,
      UNNEST(JSON_QUERY_ARRAY(raw_json_data)) AS product,
      UNNEST(JSON_QUERY_ARRAY(product, '$.variants')) AS variant
      WHERE scraped_date = '2025-09-06'
    )
    SELECT COUNT(*) as variant_count
    FROM simplytek_variants
    """
    
    result = list(client.query(query).result())[0]
    print(f"simplytek variants: {result.variant_count}")
    
    # Check if we have DimVariant data
    query = "SELECT COUNT(*) as total FROM `price-pulse-470211.warehouse.DimVariant`"
    result = list(client.query(query).result())[0]
    print(f"\nCurrent DimVariant total: {result.total}")

if __name__ == "__main__":
    main()
