"""
DimShop Transformation Script
Extracts unique shops from staging JSON data and adds only new shops to the warehouse.
"""

import json
from datetime import datetime
from typing import List, Dict, Set
import logging
from google.cloud import bigquery
from ..utils.transformation_utils import TransformationBase, generate_md5_id

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DimShopTransformer(TransformationBase):
    """
    Handles the transformation and loading of shop dimension data.
    Only adds new shops that don't already exist in the warehouse.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211"):
        """
        Initialize the DimShop transformer.
        
        Args:
            project_id: Google Cloud project ID
        """
        super().__init__(project_id)
        self.table_name = "DimShop"
        
    def extract_shops_from_staging(self) -> Set[str]:
        """
        Extract unique shop names from all staging tables.
        
        Returns:
            Set of unique shop names found in staging data
        """
        all_shops = set()
        
        # Get all staging tables
        staging_tables = [
            "stg_raw_simplytek",
            "stg_raw_appleme", 
            "stg_raw_onei_lk"
        ]
        
        for table_name in staging_tables:
            try:
                # Query to extract shop names from JSON data
                query = f"""
                SELECT DISTINCT
                    JSON_EXTRACT_SCALAR(raw_json_data, '$.shop_name') as shop_name
                FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE JSON_EXTRACT_SCALAR(raw_json_data, '$.shop_name') IS NOT NULL
                """
                
                results = self.client.query(query).result()
                
                for row in results:
                    if row.shop_name:
                        all_shops.add(row.shop_name.strip())
                        
                logger.info(f"Extracted shops from {table_name}")
                
            except Exception as e:
                logger.warning(f"Could not extract shops from {table_name}: {e}")
                continue
                
        logger.info(f"Found {len(all_shops)} unique shops in staging: {sorted(all_shops)}")
        return all_shops
    
    def get_existing_shops(self) -> Set[str]:
        """
        Get shop names that already exist in DimShop.
        
        Returns:
            Set of existing shop names
        """
        try:
            query = f"""
            SELECT shop_name
            FROM `{self.get_table_ref(self.table_name)}`
            """
            
            results = self.client.query(query).result()
            existing_shops = {row.shop_name for row in results}
            
            logger.info(f"Found {len(existing_shops)} existing shops in warehouse: {sorted(existing_shops)}")
            return existing_shops
            
        except Exception as e:
            # Table might be empty or not exist yet
            logger.info(f"No existing shops found (table empty or doesn't exist): {e}")
            return set()
    
    def get_next_shop_id(self) -> int:
        """
        Get the next available shop_id (sequential: 0, 1, 2, 3...).
        
        Returns:
            Next sequential shop_id to use
        """
        try:
            query = f"""
            SELECT COALESCE(MAX(shop_id), -1) + 1 as next_id
            FROM `{self.get_table_ref(self.table_name)}`
            """
            
            result = list(self.client.query(query).result())
            next_id = result[0].next_id if result else 0
            logger.info(f"Next shop_id will be: {next_id}")
            return next_id
            
        except Exception as e:
            # Table might be empty or not exist yet, start from 0
            logger.info(f"Starting shop_id from 0 (table empty or doesn't exist): {e}")
            return 0
    
    def generate_shop_records(self, new_shop_names: Set[str]) -> List[Dict]:
        """
        Generate shop dimension records for new shops.
        
        Args:
            new_shop_names: Set of new shop names to add
            
        Returns:
            List of shop dimension records
        """
        shop_records = []
        next_shop_id = self.get_next_shop_id()
        
        for i, shop_name in enumerate(sorted(new_shop_names)):  # Sort for consistent ordering
            shop_id = next_shop_id + i  # Sequential IDs: 0, 1, 2, 3...
            
            # Try to extract website URL from shop name
            website_url = self._generate_website_url(shop_name)
            
            shop_record = {
                'shop_id': shop_id,
                'shop_name': shop_name,
                'website_url': website_url,
                'contact_phone': None,      # Can be populated later manually
                'contact_whatsapp': None    # Can be populated later manually
            }
            
            shop_records.append(shop_record)
            
        logger.info(f"Generated {len(shop_records)} new shop records with IDs {next_shop_id} to {next_shop_id + len(shop_records) - 1}")
        return shop_records
    
    def _generate_website_url(self, shop_name: str) -> str:
        """
        Generate likely website URL from shop name.
        
        Args:
            shop_name: Name of the shop
            
        Returns:
            Probable website URL
        """
        # Basic logic to generate URLs
        if shop_name.endswith('.lk'):
            return f"https://{shop_name}"
        elif shop_name.endswith('.com'):
            return f"https://{shop_name}"
        else:
            # Try to guess based on common patterns
            if 'tek' in shop_name.lower():
                return f"https://{shop_name}.lk"
            else:
                return f"https://{shop_name}.com"
    
    def load_new_shops(self, shop_records: List[Dict]) -> None:
        """
        Load new shop records to BigQuery DimShop table.
        
        Args:
            shop_records: List of shop records to load
        """
        if not shop_records:
            logger.info("No new shops to load")
            return
            
        table_ref = self.get_table_ref(self.table_name)
        
        # Configure job to append new shops
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("shop_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("shop_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("contact_phone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("contact_whatsapp", "STRING", mode="NULLABLE"),
            ]
        )
        
        try:
            # Load data to BigQuery
            job = self.client.load_table_from_json(
                shop_records, table_ref, job_config=job_config
            )
            job.result()  # Wait for completion
            
            # Verify the load
            table = self.client.get_table(table_ref)
            logger.info(f"Successfully added {len(shop_records)} new shops to {table_ref}")
            logger.info(f"Total shops in DimShop: {table.num_rows}")
            
            # Log the new shops added
            for record in shop_records:
                logger.info(f"Added shop: {record['shop_name']} (ID: {record['shop_id']})")
                
        except Exception as e:
            logger.error(f"Failed to load shops to {table_ref}: {e}")
            raise
    
    def transform_and_load(self) -> None:
        """
        Complete transformation process for DimShop.
        Only processes new shops that don't already exist.
        """
        self.log_transformation_start(self.table_name)
        
        # Extract all shops from staging
        staging_shops = self.extract_shops_from_staging()
        
        if not staging_shops:
            logger.info("No shops found in staging data")
            return
            
        # Get existing shops
        existing_shops = self.get_existing_shops()
        
        # Find new shops
        new_shops = staging_shops - existing_shops
        
        if not new_shops:
            logger.info("No new shops to add - all shops already exist in DimShop")
            return
            
        logger.info(f"Found {len(new_shops)} new shops to add: {sorted(new_shops)}")
        
        # Generate and load new shop records
        shop_records = self.generate_shop_records(new_shops)
        self.load_new_shops(shop_records)
        
        self.log_transformation_complete(self.table_name, len(shop_records))

def main():
    """
    Main execution function for DimShop transformation.
    """
    try:
        transformer = DimShopTransformer()
        transformer.transform_and_load()
        
    except Exception as e:
        logger.error(f"DimShop transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()
