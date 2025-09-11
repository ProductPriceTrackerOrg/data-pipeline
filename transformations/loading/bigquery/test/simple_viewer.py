#!/usr/bin/env python3
"""
Simple data viewer for BigQuery staging tables
"""
import os
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

def view_simple_data():
    """View data in a simple format"""
    project_id = "price-pulse-470211"
    
    # Use default credentials
    old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if old_creds:
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    try:
        client = bigquery.Client(project=project_id)
        
        print(f"🛍️  BIGQUERY STAGING DATA VIEWER")
        print(f"=" * 60)
        
        # Simple query to show all data
        query = f"""
        SELECT 
            source_website,
            scrape_date,
            product_count,
            loaded_at,
            SUBSTR(TO_JSON_STRING(raw_json_data), 1, 100) as json_preview
        FROM `{project_id}.staging.*`
        ORDER BY loaded_at DESC
        """
        
        print(f"📊 Summary of all staging data:")
        print(f"-" * 60)
        
        results = client.query(query).result()
        
        total_products = 0
        
        for i, row in enumerate(results, 1):
            print(f"\n{i}. {row.source_website.upper()}")
            print(f"   📅 Date: {row.scrape_date}")
            print(f"   🛍️  Products: {row.product_count:,}")
            print(f"   ⏰ Loaded: {row.loaded_at}")
            print(f"   📄 JSON Preview: {row.json_preview}...")
            
            total_products += row.product_count
        
        print(f"\n" + "=" * 60)
        print(f"🎯 TOTAL: {total_products:,} products across {i} sources")
        
        # Show individual products using SQL UNNEST
        print(f"\n🔍 SAMPLE PRODUCTS FROM EACH SOURCE:")
        print(f"=" * 60)
        
        for source in ['appleme', 'simplytek', 'onei.lk']:
            print(f"\n📦 {source.upper()} - Sample Products:")
            print(f"-" * 40)
            
            # Clean source name for table lookup
            table_source = source.replace('.', '_')
            
            sample_query = f"""
            SELECT 
                JSON_EXTRACT_SCALAR(product, '$.name') as product_name,
                JSON_EXTRACT_SCALAR(product, '$.brand') as brand,
                JSON_EXTRACT_SCALAR(product, '$.price') as price
            FROM `{project_id}.staging.stg_raw_{table_source}`,
            UNNEST(JSON_EXTRACT_ARRAY(raw_json_data)) as product
            LIMIT 5
            """
            
            try:
                sample_results = client.query(sample_query).result()
                
                for j, product in enumerate(sample_results, 1):
                    name = product.product_name or "Unknown"
                    brand = product.brand or "Unknown"
                    price = product.price or "N/A"
                    print(f"   {j}. {name}")
                    print(f"      Brand: {brand}")
                    print(f"      Price: Rs. {price}")
                
            except Exception as e:
                print(f"   ❌ Error getting sample products: {e}")
        
        # Show partition info
        print(f"\n🗂️  PARTITION INFORMATION:")
        print(f"=" * 60)
        
        for source in ['appleme', 'simplytek', 'onei_lk']:
            table_id = f"{project_id}.staging.stg_raw_{source}"
            try:
                table_ref = client.get_table(table_id)
                expiration_days = table_ref.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000)
                print(f"📊 {source}: {expiration_days:.0f} days retention ✅")
            except Exception as e:
                print(f"❌ {source}: Error getting partition info")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    view_simple_data()
