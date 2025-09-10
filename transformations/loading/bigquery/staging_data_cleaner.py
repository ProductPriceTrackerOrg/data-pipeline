#!/usr/bin/env python3
"""
Staging Data Cleaner
This script cleans the raw JSON data in BigQuery staging tables for the most recent date available.
It fetches the raw JSON, cleans it in Python, and overwrites the BigQuery partition with the clean data.
This approach is highly robust and avoids complex SQL JSON parsing issues.
"""
import logging
import time
import json
from google.cloud import bigquery
from typing import Dict, Optional, List
from datetime import date, datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StagingDataCleaner:
    """
    Orchestrates the targeted cleaning of the latest data in BigQuery staging tables using a Python-based cleaning process.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211", staging_dataset: str = "staging"):
        """Initialize the Staging Data Cleaner."""
        self.project_id = project_id
        self.staging_dataset = staging_dataset
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"StagingDataCleaner initialized for project '{self.project_id}' and dataset '{self.staging_dataset}'.")

    def _get_latest_scrape_date(self, table_id: str) -> Optional[str]:
        """Finds the most recent scrape_date in the table that has data."""
        query = f"SELECT FORMAT_DATE('%Y-%m-%d', MAX(scrape_date)) as latest_date FROM `{table_id}`"
        try:
            result = list(self.client.query(query).result())
            if result and result[0].latest_date:
                latest_date = result[0].latest_date
                logger.info(f"Latest scrape date found in {table_id}: {latest_date}")
                return latest_date
        except Exception as e:
            logger.error(f"❌ Could not determine latest scrape date for {table_id}: {e}")
        
        logger.warning(f"No scrape dates found for {table_id}.")
        return None

    def _fetch_row_to_clean(self, table_id: str, date_to_check: str) -> Optional[bigquery.Row]:
        """Fetches the entire row for a specific date to be cleaned in Python."""
        logger.info(f"Fetching row for date {date_to_check} to clean...")
        query = f"SELECT * FROM `{table_id}` WHERE scrape_date = '{date_to_check}' LIMIT 1"
        try:
            rows = list(self.client.query(query).result())
            if rows:
                logger.info(f"Successfully fetched row for {date_to_check}.")
                return rows[0]
        except Exception as e:
            logger.error(f"❌ Failed to fetch row for {date_to_check}: {e}")
        
        logger.warning(f"No row found for {date_to_check}.")
        return None
    
    def _is_valid_price(self, price) -> bool:
        """Helper function to validate a price value."""
        if price is None:
            return False
        price_str = str(price).strip().lower()
        if price_str in ('', 'null', 'none', 'nan', 'n/a', 'na'):
            return False
        try:
            return float(price_str) > 0
        except (ValueError, TypeError):
            return False

    def _clean_products_in_python(self, raw_json_data: str) -> Optional[List[Dict]]:
        """
        Parses a JSON string and cleans the product list according to the price rules.
        """
        try:
            products = json.loads(raw_json_data)
            if not isinstance(products, list):
                logger.warning("JSON data is not a list. Cannot clean.")
                return None
            
            cleaned_products = []
            for product in products:
                if not isinstance(product, dict) or 'variants' not in product or not isinstance(product.get('variants'), list):
                    continue # Skip malformed products

                valid_variants = [v for v in product['variants'] if self._is_valid_price(v.get('price_current'))]
                
                # Only keep the product if it has at least one valid variant
                if valid_variants:
                    product['variants'] = valid_variants
                    cleaned_products.append(product)
            
            return cleaned_products
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ CRITICAL: Failed to parse JSON in Python. Error: {e}")
            return None

    def _overwrite_partition_with_cleaned_data(self, table_id: str, original_row: bigquery.Row, cleaned_products: List[Dict]) -> bool:
        """
        Overwrites the partition for the given date with the new, cleaned data.
        """
        date_to_overwrite = original_row['scrape_date'].strftime('%Y-%m-%d')
        logger.info(f"Preparing to overwrite partition for {date_to_overwrite} with {len(cleaned_products)} cleaned products...")

        # Construct the new row, ensuring all fields are JSON serializable
        new_row = {}
        for key, value in original_row.items():
            if isinstance(value, (date, datetime)):
                new_row[key] = value.isoformat()
            else:
                new_row[key] = value
        
        new_row['raw_json_data'] = json.dumps(cleaned_products)
        new_row['product_count'] = len(cleaned_products)
        
        table = self.client.get_table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=table.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="scrape_date",
            ),
            # --- DEFINITIVE FIX: Added the missing clustering specification ---
            clustering_fields=["source_website"],
        )

        try:
            # The destination for a load job targeting a partition is table_id$YYYYMMDD
            destination_partition = f"{table_id}${date_to_overwrite.replace('-', '')}"
            
            load_job = self.client.load_table_from_json(
                [new_row],
                destination_partition,
                job_config=job_config,
            )
            load_job.result()
            logger.info(f"Successfully overwrote partition for {date_to_overwrite} in {table_id}.")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to load cleaned data to BigQuery for {date_to_overwrite}: {e}")
        
        return False

    def run(self) -> None:
        """
        Discovers all staging tables, finds the latest date in each, and runs the cleaning process.
        """
        logger.info("Starting staging data cleaning process...")
        try:
            tables = self.client.list_tables(self.staging_dataset)
            staging_table_ids = [f"{table.project}.{table.dataset_id}.{table.table_id}" for table in tables]
            
            if not staging_table_ids:
                logger.warning("No staging tables found. Nothing to clean.")
                return

            logger.info(f"Discovered {len(staging_table_ids)} staging tables to process.")
            
            success_count = 0
            for table_id in staging_table_ids:
                logger.info(f"\n--- Processing table: {table_id} ---")
                date_to_clean = self._get_latest_scrape_date(table_id)
                
                if not date_to_clean:
                    logger.warning(f"No date found for {table_id}. Skipping.")
                    continue
                
                row_to_clean = self._fetch_row_to_clean(table_id, date_to_clean)
                if not row_to_clean:
                    logger.warning(f"Could not fetch row for {date_to_clean}. Skipping.")
                    continue
                
                original_product_count = row_to_clean.get('product_count', 0)
                cleaned_products = self._clean_products_in_python(row_to_clean['raw_json_data'])
                
                if cleaned_products is None:
                    logger.error("Cleaning failed due to JSON parsing error. Skipping table.")
                    continue
                
                cleaned_product_count = len(cleaned_products)
                products_removed = original_product_count - cleaned_product_count
                
                logger.info(f"Cleaning Summary for {table_id} on {date_to_clean}:")
                logger.info(f"  - Products before: {original_product_count}")
                logger.info(f"  - Products after:  {cleaned_product_count}")
                logger.info(f"  - Products removed:  {products_removed}")
                
                if products_removed > 0:
                    if self._overwrite_partition_with_cleaned_data(table_id, row_to_clean, cleaned_products):
                        success_count += 1
                else:
                    logger.info("No products needed to be removed. Skipping overwrite.")
                    success_count += 1

            logger.info("\n--- Overall Cleaning Summary ---")
            logger.info(f"Total tables processed: {len(staging_table_ids)}")
            logger.info(f"Successfully processed (cleaned or verified clean): {success_count}")
            logger.info("Staging data cleaning process completed.")

        except Exception as e:
            logger.error(f"❌ An error occurred during the cleaning process: {e}")

def main():
    """Main execution function for the staging data cleaner."""
    try:
        cleaner = StagingDataCleaner()
        cleaner.run()
    except Exception as e:
        logger.error(f"❌ The staging cleaner script failed to run: {e}", exc_info_True)
        raise

if __name__ == "__main__":
    main()

