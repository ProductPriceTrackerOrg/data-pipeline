"""
Quick check to see what dates we have data for in staging tables
"""
from google.cloud import bigquery

def check_staging_dates():
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔍 Checking available dates in staging tables")
    print("=" * 50)
    
    tables = ["stg_raw_appleme", "stg_raw_simplytek", "stg_raw_onei_lk"]
    
    for table in tables:
        try:
            query = f"""
            SELECT 
                scrape_date,
                COUNT(*) as product_count
            FROM `price-pulse-470211.staging.{table}`
            GROUP BY scrape_date
            ORDER BY scrape_date DESC
            LIMIT 5
            """
            
            print(f"\n📋 {table}:")
            results = client.query(query).result()
            
            for row in results:
                print(f"   {row.scrape_date}: {row.product_count} products")
                
            # Check if we have any data at all
            count_query = f"SELECT COUNT(*) as total FROM `price-pulse-470211.staging.{table}`"
            total = list(client.query(count_query).result())[0].total
            if total == 0:
                print(f"   ⚠️ No data found in {table}")
                
        except Exception as e:
            print(f"   ❌ Error checking {table}: {e}")

if __name__ == "__main__":
    check_staging_dates()
