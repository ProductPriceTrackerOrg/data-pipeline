#!/usr/bin/env python3
"""
Query and view BigQuery staging data
"""
import os
import json
from pathlib import Path
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
project_root = Path(__file__).parent.parent.parent.parent
env_path = project_root / '.env'
load_dotenv(env_path)

def view_staging_data():
    """View data in BigQuery staging tables"""
    project_id = "price-pulse-470211"
    
    # Use default credentials
    old_creds = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if old_creds:
        os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)
    
    try:
        client = bigquery.Client(project=project_id)
        
        print(f"🔍 Viewing BigQuery Staging Data")
        print(f"=" * 60)
        
        # List all staging tables
        dataset_ref = client.dataset("staging")
        tables = list(client.list_tables(dataset_ref))
        
        if not tables:
            print("❌ No tables found in staging dataset")
            return
        
        print(f"📊 Found {len(tables)} staging tables:")
        for table in tables:
            print(f"   - {table.table_id}")
        
        print(f"\n" + "=" * 60)
        
        # Query each table
        for table in tables:
            table_id = f"{project_id}.staging.{table.table_id}"
            
            print(f"\n🔍 Table: {table.table_id}")
            print(f"-" * 40)
            
            # Get table info
            table_ref = client.get_table(table_id)
            print(f"📅 Created: {table_ref.created}")
            print(f"📊 Rows: {table_ref.num_rows:,}")
            print(f"💾 Size: {table_ref.num_bytes:,} bytes")
            
            # Check partition info
            if table_ref.time_partitioning:
                expiration_days = table_ref.time_partitioning.expiration_ms / (24 * 60 * 60 * 1000)
                print(f"🗓️  Partitioned by: {table_ref.time_partitioning.field}")
                print(f"⏰ Partition expiration: {expiration_days:.0f} days")
            
            # Query summary data
            summary_query = f"""
            SELECT 
                source_website,
                scrape_date,
                product_count,
                loaded_at,
                file_path
            FROM `{table_id}`
            ORDER BY loaded_at DESC
            LIMIT 5
            """
            
            print(f"\n📋 Summary Data:")
            try:
                results = client.query(summary_query).result()
                
                for i, row in enumerate(results):
                    print(f"   Row {i+1}:")
                    print(f"      Source: {row.source_website}")
                    print(f"      Date: {row.scrape_date}")
                    print(f"      Products: {row.product_count:,}")
                    print(f"      Loaded: {row.loaded_at}")
                    print(f"      File: {row.file_path}")
                    
            except Exception as e:
                print(f"   ❌ Error querying summary: {e}")
            
            # Sample products from JSON data
            sample_query = f"""
            SELECT 
                source_website,
                JSON_EXTRACT_ARRAY(raw_json_data) as products
            FROM `{table_id}`
            LIMIT 1
            """
            
            print(f"\n🛍️  Sample Products:")
            try:
                results = client.query(sample_query).result()
                
                for row in results:
                    products = json.loads(row.products[0]) if row.products else []
                    print(f"   Total products in JSON: {len(products)}")
                    
                    # Show first 3 products
                    for i, product in enumerate(products[:3]):
                        name = product.get('name', 'Unknown')
                        brand = product.get('brand', 'Unknown')
                        price = product.get('price', 'N/A')
                        print(f"      {i+1}. {name} - {brand} - Rs. {price}")
                    
                    if len(products) > 3:
                        print(f"      ... and {len(products) - 3} more products")
                        
            except Exception as e:
                print(f"   ❌ Error querying products: {e}")
        
        # Overall summary
        print(f"\n" + "=" * 60)
        print(f"📊 OVERALL SUMMARY")
        print(f"=" * 60)
        
        total_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            SUM(product_count) as total_products,
            COUNT(DISTINCT source_website) as unique_sources,
            MIN(scrape_date) as earliest_date,
            MAX(scrape_date) as latest_date
        FROM `{project_id}.staging.*`
        """
        
        try:
            results = client.query(total_query).result()
            for row in results:
                print(f"📊 Total BigQuery rows: {row.total_rows}")
                print(f"🛍️  Total products: {row.total_products:,}")
                print(f"🌐 Unique sources: {row.unique_sources}")
                print(f"📅 Date range: {row.earliest_date} to {row.latest_date}")
                
        except Exception as e:
            print(f"❌ Error getting summary: {e}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    view_staging_data()
