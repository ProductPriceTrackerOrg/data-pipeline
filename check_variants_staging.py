"""
Quick check to see if variants exist in today's staging data
"""
from google.cloud import bigquery
from datetime import date

def check_variants_in_staging():
    client = bigquery.Client(project="price-pulse-470211")
    today = date.today().strftime("%Y-%m-%d")
    
    print(f"🔍 Checking for variants in staging data for {today}")
    print("=" * 60)
    
    # Check each staging table
    tables = ['stg_raw_appleme', 'stg_raw_simplytek', 'stg_raw_onei_lk']
    
    for table in tables:
        print(f"\n📊 {table}:")
        
        # Check total products
        count_query = f"""
        SELECT COUNT(*) as product_count
        FROM `price-pulse-470211.staging.{table}`
        WHERE scrape_date = '{today}'
        """
        
        try:
            result = list(client.query(count_query).result())[0]
            print(f"   Products: {result.product_count}")
            
            if result.product_count > 0:
                # Check if products have variants
                variant_query = f"""
                SELECT 
                    COUNT(*) as products_with_variants,
                    COUNT(CASE WHEN JSON_EXTRACT_ARRAY(products, '$.variants') IS NOT NULL 
                          AND JSON_ARRAY_LENGTH(JSON_EXTRACT_ARRAY(products, '$.variants')) > 0 
                          THEN 1 END) as products_with_variant_data
                FROM `price-pulse-470211.staging.{table}`,
                UNNEST(JSON_EXTRACT_ARRAY(products)) as products
                WHERE scrape_date = '{today}'
                """
                
                try:
                    variant_result = list(client.query(variant_query).result())[0]
                    print(f"   Products with variant data: {variant_result.products_with_variant_data}")
                except Exception as e:
                    print(f"   ❌ Error checking variants: {e}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    check_variants_in_staging()
