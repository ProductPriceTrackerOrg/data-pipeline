"""
Simple check for duplicate prevention
"""
from google.cloud import bigquery
from datetime import date

def check_duplicates():
    client = bigquery.Client(project="price-pulse-470211")
    today = date.today().strftime("%Y-%m-%d")
    
    # Count total images for today
    total_query = f"""
    SELECT COUNT(*) as total_images
    FROM `price-pulse-470211.warehouse.DimProductImage`
    WHERE scraped_date = '{today}'
    """
    
    total_result = list(client.query(total_query).result())[0]
    print(f"Total images for {today}: {total_result.total_images}")
    
    # Check for duplicates
    duplicate_query = f"""
    SELECT 
        COUNT(*) as duplicate_pairs
    FROM (
        SELECT 
            shop_product_id, 
            image_url,
            COUNT(*) as count
        FROM `price-pulse-470211.warehouse.DimProductImage`
        WHERE scraped_date = '{today}'
        GROUP BY shop_product_id, image_url
        HAVING COUNT(*) > 1
    )
    """
    
    dup_result = list(client.query(duplicate_query).result())[0]
    
    if dup_result.duplicate_pairs == 0:
        print("✅ NO DUPLICATES FOUND - Duplicate prevention is working!")
    else:
        print(f"❌ {dup_result.duplicate_pairs} duplicate pairs found")

if __name__ == "__main__":
    check_duplicates()
