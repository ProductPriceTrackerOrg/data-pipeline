"""
Test duplicate prevention in DimProductImage
"""
from google.cloud import bigquery
from datetime import date

def test_duplicate_prevention():
    """Check if duplicate prevention is working correctly"""
    client = bigquery.Client(project="price-pulse-470211")
    
    print("🔍 Testing DimProductImage Duplicate Prevention")
    print("=" * 60)
    
    today = date.today().strftime("%Y-%m-%d")
    
    # Check total images before
    before_query = f"""
    SELECT COUNT(*) as total_images
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scraped_date = '{today}'
    """
    
    try:
        before_result = list(client.query(before_query).result())[0]
        images_before = before_result.total_images
        print(f"📊 Images before test: {images_before}")
        
        # Now run the transformation again
        print("\n🔄 Running DimProductImage transformation again...")
        import subprocess
        import os
        
        os.chdir("d:\\Semester 5\\DSE Project\\data-pipeline")
        result = subprocess.run([
            "python", "-m", "transformations.warehouse.dimensions.dim_product_image"
        ], capture_output=True, text=True)
        
        print("📋 Transformation output:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Errors:")
            print(result.stderr)
        
        # Check total images after
        after_result = list(client.query(before_query).result())[0]
        images_after = after_result.total_images
        print(f"\n📊 Images after test: {images_after}")
        
        # Check if any duplicates were prevented
        if images_after == images_before:
            print("✅ DUPLICATE PREVENTION WORKING! No new images added on second run")
        else:
            print(f"❌ DUPLICATE PREVENTION FAILED! Added {images_after - images_before} new images")
            
        # Check for actual duplicates in the table
        duplicate_query = f"""
        SELECT 
            shop_product_id, 
            image_url,
            COUNT(*) as count
        FROM `price-pulse-470211.warehouse.DimProductImage`
        WHERE scraped_date = '{today}'
        GROUP BY shop_product_id, image_url
        HAVING COUNT(*) > 1
        LIMIT 5
        """
        
        duplicates = list(client.query(duplicate_query).result())
        if duplicates:
            print(f"\n⚠️ Found {len(duplicates)} actual duplicates in table:")
            for dup in duplicates:
                print(f"   Product: {dup.shop_product_id[:12]}..., URL: {dup.image_url[:50]}... (count: {dup.count})")
        else:
            print("\n✅ No actual duplicates found in table")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")

if __name__ == "__main__":
    test_duplicate_prevention()
