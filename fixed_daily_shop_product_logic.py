"""
Fixed DimShopProduct Daily Transformation Logic
Handles existing products properly by checking against warehouse and only updating scrape_date
"""

import hashlib
from datetime import date
from typing import List, Dict, Set, Tuple
from google.cloud import bigquery
import logging

class DimShopProductDailyTransformer:
    def __init__(self, project_id: str = "price-pulse-470211"):
        self.client = bigquery.Client()
        self.project_id = project_id
    
    def get_existing_shop_product_ids(self) -> Set[str]:
        """Get all existing shop_product_ids from DimShopProduct to avoid duplicates"""
        try:
            query = f"""
            SELECT DISTINCT shop_product_id 
            FROM `{self.project_id}.warehouse.DimShopProduct`
            """
            
            results = list(self.client.query(query).result())
            existing_ids = {row.shop_product_id for row in results}
            
            logging.info(f"Found {len(existing_ids)} existing shop_product_ids in DimShopProduct")
            return existing_ids
            
        except Exception as e:
            logging.warning(f"Could not fetch existing shop_product_ids (table might not exist): {e}")
            return set()
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> str:
        """Generate MD5-based shop_product_id"""
        business_key = f"{source_website}|{product_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()
    
    def separate_new_vs_existing_products(
        self, 
        products: List[Dict], 
        existing_ids: Set[str]
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Separate products into new (to insert) and existing (to potentially update scrape_date)
        
        Returns:
            Tuple of (new_products, existing_products)
        """
        new_products = []
        existing_products = []
        
        for product in products:
            shop_product_id = product.get('shop_product_id')
            
            if shop_product_id in existing_ids:
                existing_products.append(product)
            else:
                new_products.append(product)
        
        logging.info(f"Product separation: {len(new_products)} new, {len(existing_products)} existing")
        return new_products, existing_products
    
    def update_existing_products_scrape_date(
        self, 
        existing_products: List[Dict], 
        target_date: date
    ) -> bool:
        """
        Update scrape_date for existing products only if needed
        
        Args:
            existing_products: List of products that already exist
            target_date: New scrape date to update
            
        Returns:
            Success status
        """
        if not existing_products:
            logging.info("No existing products to update")
            return True
        
        try:
            # Create UPDATE statements for each existing product
            update_queries = []
            
            for product in existing_products:
                shop_product_id = product['shop_product_id']
                
                # Only update scrape_date if it's newer or different
                update_query = f"""
                UPDATE `{self.project_id}.warehouse.DimShopProduct`
                SET scraped_date = '{target_date}'
                WHERE shop_product_id = '{shop_product_id}'
                  AND (scraped_date IS NULL OR scraped_date < '{target_date}')
                """
                update_queries.append(update_query)
            
            # Execute updates in batches (BigQuery has query limits)
            batch_size = 100
            total_updated = 0
            
            for i in range(0, len(update_queries), batch_size):
                batch = update_queries[i:i + batch_size]
                
                # Combine multiple updates into a single query
                combined_query = "; ".join(batch)
                
                try:
                    job = self.client.query(combined_query)
                    job.result()
                    total_updated += len(batch)
                    
                except Exception as e:
                    logging.warning(f"Batch update failed: {e}")
                    continue
            
            logging.info(f"Updated scrape_date for {total_updated} existing products")
            return True
            
        except Exception as e:
            logging.error(f"Failed to update existing products: {e}")
            return False
    
    def insert_new_products(self, new_products: List[Dict]) -> bool:
        """
        Insert only new products to DimShopProduct
        
        Args:
            new_products: List of new products to insert
            
        Returns:
            Success status
        """
        if not new_products:
            logging.info("No new products to insert")
            return True
        
        try:
            table_id = f"{self.project_id}.warehouse.DimShopProduct"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            
            job = self.client.load_table_from_json(
                new_products, 
                table_id, 
                job_config=job_config
            )
            job.result()
            
            logging.info(f"Successfully inserted {len(new_products)} new products")
            return True
            
        except Exception as e:
            logging.error(f"Failed to insert new products: {e}")
            return False
    
    def transform_and_load_daily(self, products: List[Dict], target_date: date) -> bool:
        """
        Daily transformation process that handles existing vs new products properly
        
        Args:
            products: List of transformed product dictionaries
            target_date: Date being processed
            
        Returns:
            Success status
        """
        logging.info(f"🚀 Starting daily DimShopProduct transformation for {target_date}")
        
        # Get existing product IDs
        logging.info("🔍 Checking for existing products in warehouse...")
        existing_ids = self.get_existing_shop_product_ids()
        
        # Separate new vs existing products
        logging.info("📊 Separating new vs existing products...")
        new_products, existing_products = self.separate_new_vs_existing_products(
            products, existing_ids
        )
        
        success = True
        
        # Insert new products
        if new_products:
            logging.info(f"📥 Inserting {len(new_products)} new products...")
            success &= self.insert_new_products(new_products)
        
        # Update scrape_date for existing products
        if existing_products:
            logging.info(f"🔄 Updating scrape_date for {len(existing_products)} existing products...")
            success &= self.update_existing_products_scrape_date(existing_products, target_date)
        
        if success:
            logging.info("✅ Daily DimShopProduct transformation completed successfully!")
            logging.info(f"📊 SUMMARY:")
            logging.info(f"  - New products inserted: {len(new_products)}")
            logging.info(f"  - Existing products updated: {len(existing_products)}")
            logging.info(f"  - Total products processed: {len(products)}")
        else:
            logging.error("❌ Daily DimShopProduct transformation failed")
        
        return success

# Example of how to integrate this into the existing DimShopProduct class:
"""
Add these methods to the existing DimShopProductTransformer class:

1. Replace the load_products_to_bigquery method with the daily logic
2. Add the get_existing_shop_product_ids method
3. Add the separate_new_vs_existing_products method
4. Add the update_existing_products_scrape_date method
5. Modify transform_and_load to use the daily approach
"""
