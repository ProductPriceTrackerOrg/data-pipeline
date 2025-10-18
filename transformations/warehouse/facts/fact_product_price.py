#!/usr/bin/env python3
"""
FactProductPrice Loader Script
Extracts daily price and availability for each product variant from BigQuery staging tables and loads to warehouse.
"""
import hashlib
import json
import logging
from datetime import datetime, date, timezone
from typing import List, Dict, Optional
from google.cloud import bigquery
import xxhash

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FactProductPriceLoader:
    """
    Handles the extraction, transformation, and loading of daily price facts.
    """
    def __init__(self):
        # Set up credentials from the service account file
        import os
        from pathlib import Path
        
        # Look for the credentials file in the project root
        project_root_path = Path(__file__).parent.parent.parent.parent
        credentials_path = project_root_path / "gcp-credentials.json"
        if credentials_path.exists():
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_path)
            logger.info(f"Using service account credentials from: {credentials_path}")
        else:
            logger.warning("No credentials file found at project root. Authentication may fail.")
            
        self.project_id = "price-pulse-470211"
        self.client = bigquery.Client(project=self.project_id)
        self.staging_dataset = "staging"
        self.warehouse_dataset = "warehouse"
        self.table_name = "FactProductPrice"
        
    def _get_temp_table_ref(self) -> bigquery.TableReference:
        """Creates a reference for a temporary destination table."""
        # A temporary table will be created in the staging dataset
        # and will be automatically deleted after about 24 hours.
        temp_table_id = f"temp_extract_price_{int(datetime.now().timestamp())}"
        return self.client.dataset(self.staging_dataset).table(temp_table_id)

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

    def generate_variant_id(self, source_website: str, product_id_native: str, variant_id_native: str) -> int:
        """Generates the xxhash 32-bit integer for the variant business key."""
        business_key = f"{source_website}|{product_id_native}|{variant_id_native}"
        return xxhash.xxh32(business_key.encode('utf-8')).intdigest()

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
        Uses a temporary destination table approach to handle large result sets.
        """
        start_time = datetime.now()
        logger.info(f"Starting price fact extraction at {start_time}")
        
        staging_tables = self.get_all_staging_tables()
        if not staging_tables:
            logger.warning("No staging tables found.")
            return []

        logger.info(f"Executing transform query for date: {target_date}")
        all_facts = []
        next_id = self.get_next_fact_id()

        for table_name in staging_tables:
            try:
                # First check if the table has data for the target date
                count_query = f"""
                SELECT COUNT(*) as row_count
                FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE scrape_date = '{target_date}'
                """
                
                count_result = list(self.client.query(count_query).result())
                row_count = count_result[0].row_count if count_result else 0
                
                if row_count == 0:
                    logger.info(f"No data found in {table_name} for {target_date}")
                    continue
                
                logger.info(f"Found {row_count} rows in {table_name}, preparing extraction...")
                
                # This query selects all the necessary data for the target date
                query = f"""
                SELECT 
                    raw_json_data,
                    source_website,
                    scrape_date
                FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE scrape_date = '{target_date}'
                """
                
                # Configure the query to save results to a temporary table
                temp_table_ref = self._get_temp_table_ref()
                job_config = bigquery.QueryJobConfig(
                    destination=temp_table_ref,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                )
                
                # Start the query job and wait for it to complete
                query_job = self.client.query(query, job_config=job_config)
                logger.info(f"Running query for {table_name}, results will be stored in {temp_table_ref.table_id}")
                query_job.result()  # Waits for the job to finish.
                
                # Check how many rows were written to the destination table
                destination_table = self.client.get_table(temp_table_ref)
                if destination_table.num_rows == 0:
                    logger.info(f"No data found in {table_name} for {target_date} (temp table empty)")
                    continue
                
                logger.info(f"Query completed. Found {destination_table.num_rows} rows in temp table. Reading results...")
                
                # Read from the destination table using list_rows (uses Storage API)
                rows_iterator = self.client.list_rows(destination_table)
                table_facts_count = 0
                
                for row in rows_iterator:
                    try:
                        # Parse JSON data
                        if isinstance(row.raw_json_data, str):
                            json_data = json.loads(row.raw_json_data)
                        else:
                            json_data = row.raw_json_data
                        
                        # Sometimes json_data itself might be a string that needs parsing again (double-encoded)
                        if isinstance(json_data, str):
                            json_data = json.loads(json_data)
                            
                        # Handle JSON array of products (each row contains multiple products)
                        products_to_process = json_data if isinstance(json_data, list) else [json_data]
                        
                        for product_data in products_to_process:
                            if not isinstance(product_data, dict):
                                logger.warning(f"Expected product data to be a dictionary, got {type(product_data)}: {str(product_data)[:100]}...")
                                continue
                                
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
                logger.error(f"Error querying {table_name}: {e}", exc_info=True)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Comprehensive extraction summary
        logger.info(f"===== Price Facts Extraction Summary =====")
        logger.info(f"Total staging tables processed: {len(staging_tables)}")
        logger.info(f"Total price facts extracted: {len(all_facts)}")
        logger.info(f"Total extraction time: {duration:.2f} seconds")
        logger.info(f"Price facts per second: {len(all_facts)/duration:.1f}" if duration > 0 else "N/A")
        logger.info(f"==========================================")
        
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
                bigquery.SchemaField("variant_id", "INTEGER", mode="REQUIRED"),  # xxhash 32-bit integer
                bigquery.SchemaField("date_id", "INTEGER", mode="REQUIRED"),
                # Changed from FLOAT to NUMERIC with precision and scale to match the existing schema
                bigquery.SchemaField("current_price", "FLOAT", mode="REQUIRED"),
                bigquery.SchemaField("original_price", "FLOAT", mode="NULLABLE"),
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
    
    # Use the current UTC date for the transformation
    target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    transformed_rows = loader.extract_and_transform(target_date)
    loader.load_to_bigquery(transformed_rows)

if __name__ == "__main__":
    main()
