#!/usr/bin/env python3
"""
Test script to validate variant detection from staging data
"""

import hashlib
import json
from google.cloud import bigquery
from datetime import datetime

class VariantDetectionTest:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "price-pulse-470211"
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> str:
        """Generate MD5-based shop_product_id"""
        business_key = f"{source_website}|{product_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()
    
    def generate_variant_id(self, source_website: str, product_id_native: str, variant_id_native: str) -> str:
        """Generate MD5-based variant_id"""
        business_key = f"{source_website}|{product_id_native}|{variant_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()
    
    def get_staging_tables(self) -> list:
        """Get all staging tables"""
        query = """
        SELECT table_name
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'stg_raw_%'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        results = list(self.client.query(query).result())
        return [row.table_name for row in results]
    
    def test_variant_extraction(self, target_date: str = '2025-09-06'):
        """Test variant extraction with corrected array handling"""
        staging_tables = self.get_staging_tables()
        print(f"🔍 Testing variant extraction for {target_date}")
        print(f"Found {len(staging_tables)} staging tables")
        
        total_variants = 0
        
        for table_name in staging_tables:
            print(f"\n📋 Processing {table_name}:")
            
            # Extract source website from table name
            source_website = table_name.replace('stg_raw_', '')
            
            # Since raw_json_data contains an array of products, we need to unnest it
            query = f"""
            SELECT 
                JSON_VALUE(product, '$.product_id_native') as product_id_native,
                source_website,
                product as product_json
            FROM `{self.project_id}.staging.{table_name}`,
            UNNEST(JSON_QUERY_ARRAY(raw_json_data)) AS product
            WHERE scrape_date = '{target_date}'
            AND JSON_VALUE(product, '$.variants') IS NOT NULL
            AND ARRAY_LENGTH(JSON_QUERY_ARRAY(product, '$.variants')) > 0
            LIMIT 10
            """
            
            try:
                results = list(self.client.query(query).result())
                print(f"  Found {len(results)} products with variants")
                
                table_variants = 0
                
                for row in results:
                    try:
                        product_data = json.loads(row.product_json)
                        product_id_native = row.product_id_native
                        variants = product_data.get('variants', [])
                        
                        print(f"    Product {product_id_native}: {len(variants)} variants")
                        
                        # Generate shop_product_id
                        shop_product_id = self.generate_shop_product_id(source_website, product_id_native)
                        
                        # Process each variant
                        for variant in variants:
                            variant_id_native = variant.get('variant_id_native', '')
                            variant_title = variant.get('variant_title', '')
                            
                            variant_id = self.generate_variant_id(
                                source_website, product_id_native, variant_id_native
                            )
                            
                            print(f"      Variant: {variant_title[:50]}...")
                            print(f"        variant_id: {variant_id}")
                            print(f"        shop_product_id: {shop_product_id}")
                            
                            table_variants += 1
                            
                    except (json.JSONDecodeError, KeyError) as e:
                        print(f"      Error processing product: {e}")
                        continue
                
                total_variants += table_variants
                print(f"  Total variants extracted from {table_name}: {table_variants}")
                
            except Exception as e:
                print(f"  Error querying {table_name}: {e}")
                continue
        
        print(f"\n🎯 FINAL SUMMARY:")
        print(f"  Total variants extracted: {total_variants}")
        return total_variants

if __name__ == "__main__":
    tester = VariantDetectionTest()
    tester.test_variant_extraction()
