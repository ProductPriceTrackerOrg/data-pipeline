"""
Quick check to see what dates have data in staging tables
"""
from google.cloud import bigquery

def check_staging_data():
    """Check what dates have data in staging tables"""
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔍 Checking available dates in staging tables")
    print("=" * 50)
    
    tables = ["stg_raw_appleme", "stg_raw_simplytek", "stg_raw_onei_lk"]
    
    for table in tables:
        print(f"\n📊 {table}:")
        
        # Check available dates
        date_query = f"""
        SELECT 
            scrape_date,
            COUNT(*) as product_count,
            COUNT(DISTINCT JSON_EXTRACT_SCALAR(products, '$.product_id_native')) as unique_products
        FROM `price-pulse-470211.staging.{table}`
        GROUP BY scrape_date
        ORDER BY scrape_date DESC
        LIMIT 5
        """
        
        try:
            results = client.query(date_query).result()
            found_dates = False
            for row in results:
                print(f"   {row.scrape_date}: {row.product_count} records, ~{row.unique_products} products")
                found_dates = True
            
            if not found_dates:
                print("   ⚠️ No data found")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    check_staging_data()
