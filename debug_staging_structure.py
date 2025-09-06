#!/usr/bin/env python3
"""
Debug script to understand the actual staging table structure
"""

import hashlib
import json
from google.cloud import bigquery
from datetime import datetime

class StagingStructureDebugger:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "price-pulse-470211"
    
    def check_staging_tables_and_data(self, target_date: str = '2025-09-06'):
        """Check all staging tables and their data structure"""
        
        # First, let's see what staging tables exist
        query = """
        SELECT table_name
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'stg_raw_%'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        tables = list(self.client.query(query).result())
        print(f"📋 STAGING TABLES FOUND: {len(tables)}")
        for table in tables:
            print(f"  - {table.table_name}")
        
        # Now check each table's structure and data
        for table in tables:
            table_name = table.table_name
            print(f"\n🔍 ANALYZING TABLE: {table_name}")
            
            # Check table structure
            structure_query = f"""
            SELECT column_name, data_type
            FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            
            columns = list(self.client.query(structure_query).result())
            print(f"  📊 COLUMNS:")
            for col in columns:
                print(f"    - {col.column_name}: {col.data_type}")
            
            # Check data for target date
            data_query = f"""
            SELECT 
                source_website,
                scrape_date,
                ARRAY_LENGTH(JSON_QUERY_ARRAY(raw_json_data)) as product_count,
                JSON_VALUE(JSON_QUERY_ARRAY(raw_json_data)[OFFSET(0)], '$.product_id_native') as first_product_id,
                ARRAY_LENGTH(JSON_QUERY_ARRAY(JSON_QUERY_ARRAY(raw_json_data)[OFFSET(0)], '$.variants')) as first_product_variant_count
            FROM `price-pulse-470211.staging.{table_name}`
            WHERE scrape_date = '{target_date}'
            """
            
            try:
                data_results = list(self.client.query(data_query).result())
                print(f"  📈 DATA FOR {target_date}:")
                
                if not data_results:
                    print(f"    ❌ No data found for {target_date}")
                else:
                    for row in data_results:
                        print(f"    - Source: {row.source_website}")
                        print(f"    - Products in array: {row.product_count}")
                        print(f"    - First product ID: {row.first_product_id}")
                        print(f"    - First product variants: {row.first_product_variant_count}")
                        
            except Exception as e:
                print(f"    ❌ Error querying data: {e}")
    
    def test_variant_extraction_corrected(self, target_date: str = '2025-09-06'):
        """Test variant extraction with the correct understanding"""
        
        # Get all staging tables
        query = """
        SELECT table_name
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'stg_raw_%'
        AND table_type = 'BASE TABLE'
        """
        
        tables = list(self.client.query(query).result())
        print(f"\n🔧 TESTING VARIANT EXTRACTION FOR {target_date}")
        
        total_variants = 0
        
        for table in tables:
            table_name = table.table_name
            print(f"\n📋 Processing table: {table_name}")
            
            # For each table, iterate through all rows and unnest the JSON arrays
            extraction_query = f"""
            SELECT 
                source_website,
                JSON_VALUE(product, '$.product_id_native') as product_id_native,
                JSON_VALUE(product, '$.product_title') as product_title,
                ARRAY_LENGTH(JSON_QUERY_ARRAY(product, '$.variants')) as variant_count,
                product as product_json
            FROM `{self.project_id}.staging.{table_name}`,
            UNNEST(JSON_QUERY_ARRAY(raw_json_data)) AS product
            WHERE scrape_date = '{target_date}'
            LIMIT 10
            """
            
            try:
                results = list(self.client.query(extraction_query).result())
                print(f"  Found {len(results)} products")
                
                table_variants = 0
                
                for row in results:
                    print(f"    🛍️ Product: {row.product_id_native}")
                    print(f"       Title: {row.product_title[:50]}...")
                    print(f"       Variants: {row.variant_count}")
                    
                    # Process variants
                    try:
                        # BigQuery returns the product as a dict, not a JSON string
                        product_data = row.product_json if isinstance(row.product_json, dict) else json.loads(row.product_json)
                        variants = product_data.get('variants', [])
                        
                        for i, variant in enumerate(variants):
                            variant_id_native = variant.get('variant_id_native', '')
                            variant_title = variant.get('variant_title', '')
                            
                            # Generate IDs
                            shop_product_id = self.generate_shop_product_id(row.source_website, row.product_id_native)
                            variant_id = self.generate_variant_id(row.source_website, row.product_id_native, variant_id_native)
                            
                            print(f"         Variant {i+1}: {variant_title[:40]}...")
                            print(f"           variant_id: {variant_id[:12]}...")
                            print(f"           shop_product_id: {shop_product_id[:12]}...")
                            
                            table_variants += 1
                        
                    except Exception as e:
                        print(f"         ❌ Error processing variants: {e}")
                
                total_variants += table_variants
                print(f"  Variants extracted from {table_name}: {table_variants}")
                
            except Exception as e:
                print(f"  ❌ Error processing {table_name}: {e}")
        
        print(f"\n🎯 TOTAL VARIANTS EXTRACTED: {total_variants}")
        return total_variants
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> str:
        """Generate MD5-based shop_product_id"""
        business_key = f"{source_website}|{product_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()
    
    def generate_variant_id(self, source_website: str, product_id_native: str, variant_id_native: str) -> str:
        """Generate MD5-based variant_id"""
        business_key = f"{source_website}|{product_id_native}|{variant_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()

if __name__ == "__main__":
    debugger = StagingStructureDebugger()
    
    print("🔍 STEP 1: Understanding staging table structure")
    debugger.check_staging_tables_and_data()
    
    print("\n" + "="*80)
    print("🔧 STEP 2: Testing corrected variant extraction")
    debugger.test_variant_extraction_corrected()
