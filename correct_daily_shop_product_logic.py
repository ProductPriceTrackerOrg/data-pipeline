"""
Corrected Daily DimShopProduct Transformation Logic
Based on user requirements:
1. Check existing products by shop_product_id
2. Insert only NEW products
3. Update scrape_date for ALL products (existing + new) from staging
"""

import hashlib
from datetime import date
from typing import List, Dict, Set
from google.cloud import bigquery
import logging

def get_existing_shop_product_ids(client: bigquery.Client, project_id: str) -> Set[str]:
    """Get all existing shop_product_ids from DimShopProduct"""
    try:
        query = f"""
        SELECT DISTINCT shop_product_id 
        FROM `{project_id}.warehouse.DimShopProduct`
        """
        
        results = list(client.query(query).result())
        existing_ids = {row.shop_product_id for row in results}
        
        logging.info(f"Found {len(existing_ids)} existing shop_product_ids in DimShopProduct")
        return existing_ids
        
    except Exception as e:
        logging.warning(f"Could not fetch existing shop_product_ids (table might not exist): {e}")
        return set()

def separate_new_products(products: List[Dict], existing_ids: Set[str]) -> List[Dict]:
    """Filter out existing products, return only NEW products for insertion"""
    new_products = []
    existing_count = 0
    
    for product in products:
        shop_product_id = product.get('shop_product_id')
        
        if shop_product_id not in existing_ids:
            new_products.append(product)
        else:
            existing_count += 1
    
    logging.info(f"Product filtering: {len(new_products)} new products to insert, {existing_count} existing products skipped")
    return new_products

def insert_new_products(client: bigquery.Client, project_id: str, new_products: List[Dict]) -> bool:
    """Insert only NEW products to DimShopProduct"""
    if not new_products:
        logging.info("No new products to insert")
        return True
    
    try:
        table_id = f"{project_id}.warehouse.DimShopProduct"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        )
        
        job = client.load_table_from_json(
            new_products, 
            table_id, 
            job_config=job_config
        )
        job.result()
        
        logging.info(f"✅ Successfully inserted {len(new_products)} new products")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to insert new products: {e}")
        return False

def update_all_products_scrape_date(
    client: bigquery.Client, 
    project_id: str, 
    all_products: List[Dict], 
    target_date: date
) -> bool:
    """
    Update scrape_date for ALL products (both existing and newly inserted)
    This ensures all products reflect the latest scrape_date from staging
    """
    if not all_products:
        logging.info("No products to update scrape_date")
        return True
    
    try:
        # Get all shop_product_ids from this batch
        shop_product_ids = [f"'{product['shop_product_id']}'" for product in all_products]
        ids_list = ", ".join(shop_product_ids)
        
        # Update scrape_date for all products in this batch
        update_query = f"""
        UPDATE `{project_id}.warehouse.DimShopProduct`
        SET scraped_date = '{target_date}'
        WHERE shop_product_id IN ({ids_list})
        """
        
        job = client.query(update_query)
        result = job.result()
        
        # Get the number of updated rows
        updated_count = job.num_dml_affected_rows if hasattr(job, 'num_dml_affected_rows') else len(all_products)
        
        logging.info(f"✅ Updated scrape_date to {target_date} for {updated_count} products")
        return True
        
    except Exception as e:
        logging.error(f"❌ Failed to update scrape_date: {e}")
        return False

def daily_shop_product_transformation(
    client: bigquery.Client,
    project_id: str, 
    all_products: List[Dict], 
    target_date: date
) -> bool:
    """
    Complete daily transformation process:
    1. Check existing products
    2. Insert only NEW products  
    3. Update scrape_date for ALL products
    """
    logging.info(f"🚀 Starting daily DimShopProduct transformation for {target_date}")
    
    # Step 1: Get existing product IDs
    logging.info("🔍 Step 1: Checking existing products in warehouse...")
    existing_ids = get_existing_shop_product_ids(client, project_id)
    
    # Step 2: Separate and insert only NEW products
    logging.info("📥 Step 2: Filtering and inserting NEW products...")
    new_products = separate_new_products(all_products, existing_ids)
    
    success = True
    if new_products:
        success &= insert_new_products(client, project_id, new_products)
    
    # Step 3: Update scrape_date for ALL products (existing + new)
    logging.info(f"🔄 Step 3: Updating scrape_date for ALL {len(all_products)} products...")
    success &= update_all_products_scrape_date(client, project_id, all_products, target_date)
    
    # Summary
    if success:
        logging.info("✅ Daily DimShopProduct transformation completed successfully!")
        logging.info(f"📊 SUMMARY:")
        logging.info(f"  - Total products processed: {len(all_products)}")
        logging.info(f"  - New products inserted: {len(new_products)}")
        logging.info(f"  - Existing products found: {len(all_products) - len(new_products)}")
        logging.info(f"  - Products with updated scrape_date: {len(all_products)}")
    else:
        logging.error("❌ Daily DimShopProduct transformation failed")
    
    return success

# Integration example for existing DimShopProductTransformer class:
"""
Replace the load_products_to_bigquery method in dim_shop_product.py with:

def load_products_to_bigquery(self, products: List[Dict], target_date: date) -> bool:
    return daily_shop_product_transformation(
        self.client, 
        self.project_id, 
        products, 
        target_date
    )
"""
