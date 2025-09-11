#!/usr/bin/env python3
"""
DimVariant Transformation Script

This script transforms variant data from staging tables to the DimVariant dimension table.
For each variant, it finds the corresponding shop_product_id by matching the business key.

Business Logic:
- Each variant belongs to a shop product
- shop_product_id is found by matching source_website + product_id_native
- variant_id is generated using xxHash32 of shop_product_id + variant_id_native
- Handles multiple variants per product
- Prevents duplicates by checking existing variants in BigQuery
"""

import json
import logging
from datetime import datetime, date, timezone
from typing import List, Dict, Any
from google.cloud import bigquery
import xxhash  # Make sure xxhash is imported

logging.basicConfig(level=logging.INFO)

class DimVariantTransformation:
    def __init__(self):
        self.client = bigquery.Client()
        self.project_id = "price-pulse-470211"
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> int:
        """Generate xxhash-based shop_product_id as 32-bit integer"""
        import xxhash
        business_key = f"{source_website}|{product_id_native}"
        return xxhash.xxh32(business_key.encode('utf-8')).intdigest()
    
    def generate_variant_id(self, source_website: str, product_id_native: str, variant_id_native: str) -> int:
        """Generate xxhash-based variant_id as 32-bit integer"""
        import xxhash
        business_key = f"{source_website}|{product_id_native}|{variant_id_native}"
        return xxhash.xxh32(business_key.encode('utf-8')).intdigest()
    
    def get_all_staging_tables(self) -> List[str]:
        """Get all staging tables that contain product data"""
        query = """
        SELECT table_name
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'stg_raw_%'
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        
        results = list(self.client.query(query).result())
        tables = [row.table_name for row in results]
        logging.info(f"Discovered {len(tables)} staging tables: {tables}")
        return tables
    
    def extract_variants_from_staging(self, table_name: str, target_date: str) -> List[Dict[str, Any]]:
        """Extract variant data from a staging table"""
        
        # Use the same approach as in dim_shop_product.py
        query = f"""
        SELECT 
            raw_json_data,
            source_website,
            scrape_date
        FROM `{self.project_id}.staging.{table_name}`
        WHERE scrape_date = '{target_date}'
        """
        
        try:
            results = list(self.client.query(query).result())
            variants = []
            
            for row in results:
                try:
                    # Parse JSON data
                    if isinstance(row.raw_json_data, str):
                        json_data = json.loads(row.raw_json_data)
                    else:
                        json_data = row.raw_json_data
                    
                    # Handle JSON array of products (each row contains multiple products)
                    products_to_process = json_data if isinstance(json_data, list) else [json_data]
                    
                    for product_data in products_to_process:
                        source_website = row.source_website
                        product_id_native = product_data.get('product_id_native', '')
                        
                        # Skip if no product_id_native
                        if not product_id_native:
                            continue
                            
                        shop_product_id = self.generate_shop_product_id(source_website, product_id_native)
                        
                        # Process variants for this product
                        for variant_data in product_data.get('variants', []):
                            variant_info = {
                                'shop_product_id': shop_product_id,
                                'source_website': source_website,
                                'product_id_native': product_id_native,
                                'variant_data': variant_data
                            }
                            variants.append(variant_info)
                        
                except (json.JSONDecodeError, KeyError, AttributeError) as e:
                    logging.warning(f"Error parsing product data: {e}")
                    continue
            
            logging.info(f"Extracted {len(variants)} variants from {table_name}")
            return variants
            
        except Exception as e:
            logging.error(f"Error querying {table_name}: {e}")
            return []
    
    def transform_variant(self, variant_info: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a single variant to DimVariant format"""
        variant_data = variant_info['variant_data']
        
        # Generate variant_id using business key
        variant_id = self.generate_variant_id(
            variant_info['source_website'],
            variant_info['product_id_native'],
            variant_data.get('variant_id_native', '')
        )
        
        return {
            'variant_id': variant_id,
            'shop_product_id': variant_info['shop_product_id'],
            'variant_title': variant_data.get('variant_title', '').strip()
        }
    
    def get_existing_variant_ids(self) -> set:
        """Get all existing variant_ids from DimVariant table to avoid duplicates"""
        try:
            # Get all existing variant_ids - we don't filter by date since variant_id should be globally unique
            query = "SELECT DISTINCT variant_id FROM `price-pulse-470211.warehouse.DimVariant`"
            
            results = list(self.client.query(query).result())
            existing_ids = {row.variant_id for row in results}
            
            logging.info(f"Found {len(existing_ids)} existing variant_ids in DimVariant table")
            return existing_ids
            
        except Exception as e:
            logging.warning(f"Could not fetch existing variant_ids (table might not exist): {e}")
            return set()
    
    def deduplicate_variants(self, variants: List[Dict[str, Any]], existing_variant_ids: set = None) -> List[Dict[str, Any]]:
        """Remove duplicate variants based on variant_id and check against existing data"""
        if existing_variant_ids is None:
            existing_variant_ids = set()
            
        seen = set()
        unique_variants = []
        duplicates_in_batch = 0
        already_exists = 0
        
        for variant in variants:
            variant_id = variant['variant_id']
            
            # Check if already exists in BigQuery
            if variant_id in existing_variant_ids:
                already_exists += 1
                continue
                
            # Check if duplicate within this batch
            if variant_id in seen:
                duplicates_in_batch += 1
                continue
                
            seen.add(variant_id)
            unique_variants.append(variant)
        
        logging.info(f"Deduplication results:")
        logging.info(f"  - Total input variants: {len(variants)}")
        logging.info(f"  - Already exist in BigQuery: {already_exists}")
        logging.info(f"  - Duplicates within batch: {duplicates_in_batch}")
        logging.info(f"  - New unique variants: {len(unique_variants)}")
        
        return unique_variants
    
    def load_to_bigquery(self, variants: List[Dict[str, Any]]) -> bool:
        """Load variants to BigQuery DimVariant table"""
        if not variants:
            logging.warning("No variants to load")
            return True
        
        table_id = f"{self.project_id}.warehouse.DimVariant"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            schema=[
                bigquery.SchemaField("variant_id", "INTEGER", mode="REQUIRED"),  # xxhash 32-bit integer
                bigquery.SchemaField("shop_product_id", "INTEGER", mode="REQUIRED"),  # xxhash 32-bit integer
                bigquery.SchemaField("variant_title", "STRING", mode="REQUIRED")
            ]
        )
        
        try:
            job = self.client.load_table_from_json(variants, table_id, job_config=job_config)
            job.result()  # Wait for the job to complete
            
            logging.info(f"Successfully loaded {len(variants)} variants to DimVariant")
            return True
            
        except Exception as e:
            logging.error(f"Error loading variants to BigQuery: {e}")
            return False
    
    def transform_and_load(self, target_date: str = None):
        """Run the complete DimVariant transformation"""
        if target_date is None:
            target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        print(f"🚀 Starting DimVariant transformation for {target_date}")
        
        # First, get existing variant_ids to avoid duplicates
        print("🔍 Checking for existing variants in BigQuery...")
        existing_variant_ids = self.get_existing_variant_ids()
        
        # Get all staging tables
        staging_tables = self.get_all_staging_tables()
        
        all_variants = []
        
        # Process each staging table
        for table_name in staging_tables:
            print(f"\n📋 Processing {table_name}...")
            
            # Extract variants from this table
            raw_variants = self.extract_variants_from_staging(table_name, target_date)
            
            # Transform each variant
            transformed_variants = []
            for variant_info in raw_variants:
                try:
                    transformed_variant = self.transform_variant(variant_info)
                    transformed_variants.append(transformed_variant)
                except Exception as e:
                    logging.warning(f"Error transforming variant: {e}")
                    continue
            
            print(f"  ✅ Transformed {len(transformed_variants)} variants")
            all_variants.extend(transformed_variants)
        
        # Deduplicate variants (both within batch and against existing data)
        print(f"\n🔄 Deduplicating {len(all_variants)} variants against existing data...")
        unique_variants = self.deduplicate_variants(all_variants, existing_variant_ids)
        
        # Load to BigQuery only if we have new variants
        if unique_variants:
            print(f"\n📤 Loading {len(unique_variants)} new variants to BigQuery...")
            success = self.load_to_bigquery(unique_variants)
        else:
            print(f"\n✅ No new variants to load - all variants already exist in BigQuery")
            success = True
        
        if success:
            print(f"✅ DimVariant transformation completed successfully!")
            print(f"📊 SUMMARY:")
            print(f"  - Staging tables processed: {len(staging_tables)}")
            print(f"  - Total variants extracted: {len(all_variants)}")
            print(f"  - Existing variants in BigQuery: {len(existing_variant_ids)}")
            print(f"  - New unique variants loaded: {len(unique_variants)}")
        else:
            print("❌ DimVariant transformation failed during BigQuery loading")
        
        return success


def main():
    """Main execution function"""
    transformer = DimVariantTransformation()
    # By calling transform_and_load without a date, it will automatically
    # use the current UTC date as per the logic inside the function.
    success = transformer.transform_and_load()
    
    if success:
        logging.info("DimVariant transformation completed successfully")
    else:
        logging.error("DimVariant transformation failed")
        exit(1)


if __name__ == "__main__":
    main()
