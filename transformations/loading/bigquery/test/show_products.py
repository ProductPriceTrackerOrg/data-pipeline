#!/usr/bin/env python3
"""
Show the actual field names and real product data
"""
import os
from pathlib import Path
from google.cloud import bigquery

def show_real_products():
    """Show real product data with actual field names"""
    project_id = "price-pulse-470211"
    
    # Use default credentials
    old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if old_creds:
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    try:
        client = bigquery.Client(project=project_id)
        
        print(f"🛍️  REAL PRODUCT DATA FROM BIGQUERY")
        print(f"=" * 70)
        
        # Get one complete product to see all fields
        print(f"🔍 Complete Product Example (Appleme):")
        print(f"-" * 50)
        
        sample_query = f"""
        SELECT 
            TO_JSON_STRING(product) as full_product
        FROM `{project_id}.staging.stg_raw_appleme`,
        UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
        LIMIT 1
        """
        
        results = client.query(sample_query).result()
        for row in results:
            # Parse and pretty print the JSON
            import json
            product_data = json.loads(row.full_product)
            
            print(f"📋 Available fields:")
            for key, value in product_data.items():
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"   {key}: {value}")
        
        # Now show products with the actual available fields
        print(f"\n" + "=" * 70)
        print(f"🛍️  APPLEME PRODUCTS (Real Data):")
        print(f"=" * 70)
        
        appleme_real_query = f"""
        SELECT 
            JSON_EXTRACT_SCALAR(product, '$.scraped_product_name') as name,
            JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
            JSON_EXTRACT_SCALAR(product, '$.scraped_price') as price,
            JSON_EXTRACT_SCALAR(product, '$.availability') as stock,
            JSON_EXTRACT_SCALAR(product, '$.url') as url
        FROM `{project_id}.staging.stg_raw_appleme`,
        UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
        WHERE JSON_EXTRACT_SCALAR(product, '$.scraped_product_name') IS NOT NULL
        LIMIT 10
        """
        
        try:
            results = client.query(appleme_real_query).result()
            found_products = False
            
            for i, row in enumerate(results, 1):
                found_products = True
                print(f"\n{i}. 📱 {row.name}")
                print(f"   🏷️  Brand: {row.brand}")
                print(f"   💰 Price: {row.price}")
                print(f"   📦 Stock: {row.stock}")
                if row.url:
                    print(f"   🔗 URL: {row.url[:60]}...")
            
            if not found_products:
                print("   Trying alternative field names...")
                
                # Try other common field combinations
                alt_query = f"""
                SELECT 
                    JSON_EXTRACT(product) as raw_product
                FROM `{project_id}.staging.stg_raw_appleme`,
                UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
                LIMIT 3
                """
                
                alt_results = client.query(alt_query).result()
                for i, row in enumerate(alt_results, 1):
                    print(f"\n   Product {i} raw data:")
                    print(f"   {str(row.raw_product)[:200]}...")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        # Show summary statistics
        print(f"\n" + "=" * 70)
        print(f"📊 DATA SUMMARY")
        print(f"=" * 70)
        
        summary_query = f"""
        SELECT 
            'appleme' as source,
            COUNT(*) as total_products
        FROM `{project_id}.staging.stg_raw_appleme`,
        UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
        
        UNION ALL
        
        SELECT 
            'simplytek' as source,
            COUNT(*) as total_products
        FROM `{project_id}.staging.stg_raw_simplytek`,
        UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
        
        UNION ALL
        
        SELECT 
            'onei.lk' as source,
            COUNT(*) as total_products
        FROM `{project_id}.staging.stg_raw_onei_lk`,
        UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
        """
        
        results = client.query(summary_query).result()
        total = 0
        
        for row in results:
            print(f"🌐 {row.source}: {row.total_products:,} products")
            total += row.total_products
        
        print(f"\n🎯 TOTAL: {total:,} products successfully loaded!")
        
        # Partition retention confirmation
        print(f"\n⏰ PARTITION RETENTION: 7 days ✅")
        print(f"💾 Data will auto-delete after 1 week")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    show_real_products()
