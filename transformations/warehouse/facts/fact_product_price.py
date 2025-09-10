#!/usr/bin/env python3
"""
FactProductPrice Loader Script
Extracts daily price and availability for each product variant from BigQuery staging tables and loads to warehouse.
"""
import hashlib
import json
import logging
from datetime import datetime, date
from typing import List, Dict, Optional
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FactProductPriceLoader:
    """
    Handles the extraction, transformation, and loading of daily price facts.
    """
    def __init__(self):
        self.client = bigquery.Client(project="price-pulse-470211")
        self.project_id = "price-pulse-470211"
        self.warehouse_dataset = "warehouse"
        self.table_name = "FactProductPrice"

    def get_all_staging_tables(self) -> List[str]:
        """Dynamically discovers all staging tables."""
        query = """
        SELECT table_name
        FROM `price-pulse-470211.staging.INFORMATION_SCHEMA.TABLES`
        WHERE table_name LIKE 'stg_raw_%' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        try:
            results = self.client.query(query).result()
            tables = [row.table_name for row in results]
            logger.info(f"Discovered {len(tables)} staging tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Failed to discover staging tables: {e}")
            return []

    def get_next_fact_id(self) -> int:
        """Gets the next available price_fact_id from the warehouse table."""
        query = f"""
        SELECT COALESCE(MAX(price_fact_id), 0) + 1 as next_id
        FROM `{self.project_id}.{self.warehouse_dataset}.{self.table_name}`
        """
        try:
            result = list(self.client.query(query).result())
            return result[0].next_id if result else 1
        except Exception:
            logger.warning(f"Could not read from {self.table_name}. Starting price_fact_id from 1.")
            return 1

    def generate_variant_id(self, source_website: str, product_id_native: str, variant_id_native: str) -> str:
        """Generates the MD5 hash for the variant business key."""
        business_key = f"{source_website}|{product_id_native}|{variant_id_native}"
        return hashlib.md5(business_key.encode('utf-8')).hexdigest()

    def parse_price(self, price_str: str) -> Optional[float]:
        """Cleans and converts a price string to a float."""
        if not price_str:
            return None
        try:
            # Remove currency symbols, commas, and strip whitespace
            cleaned_str = str(price_str).replace('Rs.', '').replace('LKR', '').replace(',', '').strip()
            return float(cleaned_str)
        except (ValueError, TypeError):
            return None

    def is_available(self, text: str) -> bool:
        """
        Simplified availability check that prioritizes:
        - 'out' words indicate unavailable (false)
        - 'in' words indicate available (true)
        
        Args:
            text: Availability text string
            
        Returns:
            True if available, False otherwise
        """
        if not text:
            return False
            
        # Convert to lowercase for case-insensitive matching
        text_lower = text.lower()
        
        # First check for 'out' which indicates unavailable
        if 'out' in text_lower:
            return False
            
        # Then check for 'in' which indicates available
        if 'in' in text_lower:
            return True
            
        # Default to unavailable if neither pattern is found
        return False

    def extract_and_transform(self, target_date: str) -> List[Dict]:
        """
        Extracts price data from staging tables and transforms it for the fact table.
        Now using the approach that worked for dim_shop_product.py
        """
        staging_tables = self.get_all_staging_tables()
        if not staging_tables:
            logger.warning("No staging tables found.")
            return []

        logger.info(f"Executing transform query for date: {target_date}")
        all_facts = []
        next_id = self.get_next_fact_id()

        for table_name in staging_tables:
            try:
                # Direct extraction of raw JSON data
                query = f"""
                SELECT 
                    raw_json_data,
                    source_website,
                    scrape_date
                FROM `{self.project_id}.staging.{table_name}`
                WHERE scrape_date = '{target_date}'
                """
                
                results = self.client.query(query).result()
                table_facts_count = 0
                
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
                            # Basic validation
                            product_id_native = product_data.get('product_id_native')
                            if not product_id_native:
                                continue
                                
                            # Process each variant for this product
                            variants = product_data.get('variants', [])
                            for variant in variants:
                                variant_id_native = variant.get('variant_id_native')
                                if not variant_id_native:
                                    continue
                                
                                # Generate variant_id
                                variant_id = self.generate_variant_id(
                                    row.source_website,
                                    product_id_native,
                                    variant_id_native
                                )
                                
                                # Convert date string to date_id (YYYYMMDD)
                                date_id = int(row.scrape_date.strftime("%Y%m%d"))
                                
                                # Extract price and availability information
                                price_current = self.parse_price(variant.get('price_current'))
                                price_original = self.parse_price(variant.get('price_original'))
                                is_available = self.is_available(variant.get('availability_text', ''))
                                
                                # Skip if no price information
                                if price_current is None and price_original is None:
                                    continue
                                
                                fact = {
                                    "price_fact_id": next_id,
                                    "variant_id": variant_id,
                                    "date_id": date_id,
                                    "current_price": price_current,
                                    "original_price": price_original,
                                    "is_available": is_available
                                }
                                all_facts.append(fact)
                                table_facts_count += 1
                                next_id += 1
                                
                    except Exception as e:
                        logger.warning(f"Error processing product data: {e}")
                        continue
                
                logger.info(f"Extracted {table_facts_count} facts from {table_name}")
                
            except Exception as e:
                logger.error(f"Error querying {table_name}: {e}")
        
        logger.info(f"Successfully transformed {len(all_facts)} fact rows.")
        return all_facts

    def load_to_bigquery(self, rows: List[Dict]):
        """Loads the transformed rows into the FactProductPrice table."""
        if not rows:
            logger.info("No new rows to load.")
            return

        table_id = f"{self.project_id}.{self.warehouse_dataset}.{self.table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("price_fact_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("variant_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
                # Changed from FLOAT to NUMERIC with precision and scale to match the existing schema
                bigquery.SchemaField("current_price", "NUMERIC", mode="REQUIRED", precision=10, scale=2),
                bigquery.SchemaField("original_price", "NUMERIC", mode="NULLABLE", precision=10, scale=2),
                bigquery.SchemaField("is_available", "BOOLEAN", mode="REQUIRED"),
            ]
        )

        try:
            logger.info(f"Loading {len(rows)} rows into {table_id}...")
            job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
            job.result()
            logger.info("Load job completed successfully.")
        except Exception as e:
            logger.error(f"Failed to load data to BigQuery: {e}")
            raise

def main():
    """Main execution function."""
    loader = FactProductPriceLoader()
    
    # You can specify a date here, or it will default to today
    target_date = date(2025, 9, 8).strftime("%Y-%m-%d")
    
    transformed_rows = loader.extract_and_transform(target_date)
    loader.load_to_bigquery(transformed_rows)

if __name__ == "__main__":
    main()