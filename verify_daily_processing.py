"""
Verification script to check if our daily processing logic worked correctly
"""
from google.cloud import bigquery
from datetime import date

def verify_daily_processing():
    """Check if the daily processing logic worked as expected"""
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔍 Verifying Daily Processing Results")
    print("=" * 50)
    
    today = date.today().strftime("%Y-%m-%d")
    print(f"📅 Checking data for: {today}")
    
    # Check DimShopProduct
    print("\n📊 DimShopProduct Results:")
    shop_product_query = f"""
    SELECT 
        COUNT(*) as total_products,
        COUNT(DISTINCT shop_product_id) as unique_shop_products,
        scrape_date,
        shop_id
    FROM `price-pulse-470211.warehouse.DimShopProduct`
    WHERE scrape_date = '{today}'
    GROUP BY scrape_date, shop_id
    ORDER BY shop_id
    """
    
    try:
        results = client.query(shop_product_query).result()
        for row in results:
            print(f"   Shop {row.shop_id}: {row.total_products} products (unique: {row.unique_shop_products})")
        
        # Check total
        total_query = f"""
        SELECT COUNT(*) as total
        FROM `price-pulse-470211.warehouse.DimShopProduct`
        WHERE scrape_date = '{today}'
        """
        total_result = list(client.query(total_query).result())[0]
        print(f"   Total products today: {total_result.total}")
        
    except Exception as e:
        print(f"   ❌ Error checking DimShopProduct: {e}")
    
    # Check DimVariant
    print("\n🔍 DimVariant Results:")
    variant_query = f"""
    SELECT 
        COUNT(*) as total_variants,
        COUNT(DISTINCT variant_id) as unique_variants,
        scrape_date
    FROM `price-pulse-470211.warehouse.DimVariant`
    WHERE scrape_date = '{today}'
    GROUP BY scrape_date
    """
    
    try:
        results = client.query(variant_query).result()
        for row in results:
            print(f"   Total variants: {row.total_variants} (unique: {row.unique_variants})")
        
        if not list(results):
            print("   ⚠️ No variants found for today")
            
    except Exception as e:
        print(f"   ❌ Error checking DimVariant: {e}")
    
    # Check for duplicates (should be 0 with our new logic)
    print("\n🔍 Duplicate Check:")
    duplicate_shop_query = f"""
    SELECT shop_product_id, COUNT(*) as count
    FROM `price-pulse-470211.warehouse.DimShopProduct`
    WHERE scrape_date = '{today}'
    GROUP BY shop_product_id
    HAVING COUNT(*) > 1
    LIMIT 5
    """
    
    try:
        duplicates = list(client.query(duplicate_shop_query).result())
        if duplicates:
            print(f"   ⚠️ Found {len(duplicates)} duplicate shop products:")
            for dup in duplicates:
                print(f"      ID: {dup.shop_product_id} (count: {dup.count})")
        else:
            print("   ✅ No duplicate shop products found")
    except Exception as e:
        print(f"   ❌ Error checking duplicates: {e}")

if __name__ == "__main__":
    verify_daily_processing()
