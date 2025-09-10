"""
Verification script for DimProductImage transformation
Checks image data, sort orders, and         shop_product_id,
        sort_order,
        LEFT(image_url, 50) as image_url_preview
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scrape_date = '{today}'
    ORDER BY shop_product_id, sort_orderate prevention
"""
from google.cloud import bigquery
from datetime import date

def verify_product_images():
    """Check if the DimProductImage transformation worked correctly"""
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔍 Verifying DimProductImage Results")
    print("=" * 50)
    
    today = date.today().strftime("%Y-%m-%d")
    print(f"📅 Checking data for: {today}")
    
    # Check total images
    print("\n📊 Image Statistics:")
    total_query = f"""
    SELECT 
        COUNT(*) as total_images,
        COUNT(DISTINCT shop_product_id) as unique_products,
        COUNT(DISTINCT image_url) as unique_urls,
        MIN(image_id) as min_image_id,
        MAX(image_id) as max_image_id
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scrape_date = '{today}'
    """
    
    try:
        result = list(client.query(total_query).result())[0]
        print(f"   Total images today: {result.total_images}")
        print(f"   Unique products with images: {result.unique_products}")
        print(f"   Unique image URLs: {result.unique_urls}")
        print(f"   Image ID range: {result.min_image_id} - {result.max_image_id}")
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Check sort order logic
    print("\n🔍 Sort Order Verification:")
    sort_order_query = f"""
    SELECT 
        shop_product_id,
        COUNT(*) as image_count,
        STRING_AGG(CAST(sort_order AS STRING) ORDER BY sort_order) as sort_orders
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scrape_date = '{today}'
    GROUP BY shop_product_id
    HAVING COUNT(*) > 1
    ORDER BY image_count DESC
    LIMIT 5
    """
    
    try:
        results = list(client.query(sort_order_query).result())
        if results:
            print(f"   Products with multiple images (showing top 5):")
            for row in results:
                print(f"     Product {row.shop_product_id[:12]}...: {row.image_count} images, sort_orders: {row.sort_orders}")
        else:
            print("   ⚠️ No products found with multiple images")
            
    except Exception as e:
        print(f"   ❌ Error checking sort orders: {e}")
    
    # Check for duplicate images
    print("\n🔍 Duplicate Check:")
    duplicate_query = f"""
    SELECT 
        shop_product_id, 
        image_url,
        COUNT(*) as count
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scrape_date = '{today}'
    GROUP BY shop_product_id, image_url
    HAVING COUNT(*) > 1
    LIMIT 5
    """
    
    try:
        duplicates = list(client.query(duplicate_query).result())
        if duplicates:
            print(f"   ⚠️ Found {len(duplicates)} duplicate images:")
            for dup in duplicates:
                print(f"      Product: {dup.shop_product_id[:12]}... (count: {dup.count})")
        else:
            print("   ✅ No duplicate images found")
    except Exception as e:
        print(f"   ❌ Error checking duplicates: {e}")
    
    # Sample images
    print("\n📋 Sample Images:")
    sample_query = f"""
    SELECT 
        image_id,
        shop_product_id,
        sort_order,
        LEFT(image_url, 50) as image_url_preview
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scrape_date = '{today}'
    ORDER BY shop_product_id, sort_order
    LIMIT 5
    """
    
    try:
        samples = list(client.query(sample_query).result())
        if samples:
            for sample in samples:
                print(f"   ID: {sample.image_id}, Product: {sample.shop_product_id[:12]}..., Sort: {sample.sort_order}")
                print(f"      URL: {sample.image_url_preview}...")
        else:
            print("   ⚠️ No sample images found")
    except Exception as e:
        print(f"   ❌ Error getting samples: {e}")

if __name__ == "__main__":
    verify_product_images()
