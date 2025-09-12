import sys
import os

# Add the parent directory to path so we can import from utils
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

"""
DimShop Transformation Script
Dynamically discovers all staging tables, extracts unique shop metadata,
and incrementally loads only new shops into the warehouse.
"""

from typing import Dict, Set, List
import logging
from google.cloud import bigquery
from utils.transformation_utils import TransformationBase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DimShopTransformer(TransformationBase):
    """
    Handles the dynamic extraction, transformation, and incremental loading of shop dimension data.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211"):
        """
        Initialize the DimShop transformer.
        """
        super().__init__(project_id)
        self.table_name = "DimShop"
        
    def extract_shops_from_staging(self) -> Dict[str, Dict]:
        """
        Dynamically discovers and queries all staging tables to extract unique shop metadata.
        
        Returns:
            A dictionary where key is shop_name and value is a dict of its metadata.
            Example: {'appleme.lk': {'phone': '+94...', 'whatsapp': '+94...'}}
        """
        all_shops_metadata = {}
        
        # 1. Dynamic Table Discovery
        logger.info(f"Discovering tables in dataset '{self.staging_dataset}'...")
        staging_tables = self.client.list_tables(self.staging_dataset)
        
        for table in staging_tables:
            table_id = table.table_id
            logger.info(f"Processing staging table: {table_id}")
            try:
                # 2. Correct Data Extraction Query
                # Extracts shop name from its column and contact details from the nested JSON.
                # It only needs to check the first element of the JSON array ('$[0]') for efficiency.
                query = f"""
                SELECT DISTINCT
                    source_website AS shop_name,
                    JSON_EXTRACT_SCALAR(raw_json_data, '$[0].metadata.shop_contact_phone') AS contact_phone,
                    JSON_EXTRACT_SCALAR(raw_json_data, '$[0].metadata.shop_contact_whatsapp') AS contact_whatsapp
                FROM
                    `{self.project_id}.{self.staging_dataset}.{table_id}`
                WHERE
                    source_website IS NOT NULL AND source_website != ''
                """
                
                results = self.client.query(query).result()
                
                count = 0
                for row in results:
                    shop_name = row.shop_name.strip()
                    if shop_name and shop_name not in all_shops_metadata:
                        all_shops_metadata[shop_name] = {
                            'contact_phone': row.contact_phone,
                            'contact_whatsapp': row.contact_whatsapp
                        }
                        count += 1
                
                if count > 0:
                    logger.info(f"Found {count} unique shop(s) in {table_id}")
            
            except Exception as e:
                logger.warning(f"Could not extract shops from {table_id}: {e}")
                continue
                
        logger.info(f"Found {len(all_shops_metadata)} total unique shops across all staging tables.")
        return all_shops_metadata
    
    def get_existing_shops(self) -> Set[str]:
        """
        Gets shop names that already exist in the DimShop table.
        """
        try:
            query = f"SELECT shop_name FROM `{self.get_table_ref(self.table_name)}`"
            results = self.client.query(query).result()
            existing_shops = {row.shop_name for row in results}
            logger.info(f"Found {len(existing_shops)} existing shops in the warehouse.")
            return existing_shops
        except Exception:
            logger.info("No existing DimShop table found. Will create a new one.")
            return set()
    
    def get_next_shop_id(self) -> int:
        """
        Gets the next available sequential shop_id.
        """
        try:
            query = f"SELECT COALESCE(MAX(shop_id), -1) + 1 as next_id FROM `{self.get_table_ref(self.table_name)}`"
            result = list(self.client.query(query).result())
            return result[0].next_id if result else 0
        except Exception:
            logger.info("Starting shop_id from 0.")
            return 0
    
    def _clean_phone_number(self, phone_number: str) -> str | None:
        """
        Cleans a phone number by removing spaces and non-numeric characters,
        preserving the leading '+' for international format.
        
        Args:
            phone_number: The raw phone number string.
            
        Returns:
            A cleaned phone number string (e.g., '+94777911011') or None if input is invalid.
        """
        if not phone_number or not isinstance(phone_number, str):
            return None
        
        # Remove common formatting characters like spaces, dashes, and parentheses
        cleaned = phone_number.strip().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
        
        # A valid number should start with '+' and be followed by digits.
        if cleaned.startswith('+') and cleaned[1:].isdigit():
            return cleaned
        
        # Return None if the format is unexpected to ensure data quality
        logger.warning(f"Unexpected phone number format encountered: '{phone_number}'. Storing as NULL.")
        return None

    def generate_shop_records(self, new_shops_metadata: Dict[str, Dict]) -> List[Dict]:
        """
        Generates dimension records for the new shops.
        """
        shop_records = []
        next_shop_id = self.get_next_shop_id()
        
        # Sort for consistent ID assignment
        for i, shop_name in enumerate(sorted(new_shops_metadata.keys())):
            metadata = new_shops_metadata[shop_name]
            
            # Clean the phone numbers before creating the record
            cleaned_phone = self._clean_phone_number(metadata.get('contact_phone'))
            cleaned_whatsapp = self._clean_phone_number(metadata.get('contact_whatsapp'))
            
            shop_record = {
                'shop_id': next_shop_id + i,
                'shop_name': shop_name,
                'website_url': f"https://{shop_name}", # Generate website URL
                'contact_phone': cleaned_phone,
                'contact_whatsapp': cleaned_whatsapp
            }
            shop_records.append(shop_record)
            
        logger.info(f"Generated {len(shop_records)} new shop records.")
        return shop_records
    
    def load_new_shops(self, shop_records: List[Dict]) -> None:
        """
        Loads the new shop records to the BigQuery DimShop table.
        """
        if not shop_records:
            logger.info("No new shops to load.")
            return
            
        table_ref = self.get_table_ref(self.table_name)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("shop_id", "INT64", mode="REQUIRED"),
                bigquery.SchemaField("shop_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("website_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("contact_phone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("contact_whatsapp", "STRING", mode="NULLABLE"),
            ]
        )
        
        try:
            job = self.client.load_table_from_json(shop_records, table_ref, job_config=job_config)
            job.result()  # Wait for completion
            
            # FIX: Use table_ref directly as it's a string, not an object with a .path attribute.
            logger.info(f"Successfully loaded {job.output_rows} new shops to {table_ref}")
            for record in shop_records:
                logger.info(f"-> Added shop: {record['shop_name']} (ID: {record['shop_id']})")
                
        except Exception as e:
            # FIX: Use table_ref directly in the error message as well.
            logger.error(f"Failed to load shops to {table_ref}: {e}")
            raise
    
    def transform_and_load(self) -> None:
        """
        Orchestrates the entire transformation process for the Shop dimension.
        """
        self.log_transformation_start(self.table_name)
        
        # 1. Extract all unique shops from all staging tables
        staging_shops_metadata = self.extract_shops_from_staging()
        if not staging_shops_metadata:
            logger.info("No shops found in any staging table. Ending process.")
            self.log_transformation_complete(self.table_name, 0)
            return
            
        # 2. Get shops that are already in the warehouse
        existing_shops = self.get_existing_shops()
        
        # 3. Find the new shops that need to be added
        new_shop_names = set(staging_shops_metadata.keys()) - existing_shops
        
        if not new_shop_names:
            logger.info("No new shops to add - all shops from staging already exist in DimShop.")
            self.log_transformation_complete(self.table_name, 0)
            return
        
        logger.info(f"Found {len(new_shop_names)} new shops to add: {sorted(list(new_shop_names))}")
        
        # Filter the full metadata dict to only include new shops
        new_shops_to_add = {name: staging_shops_metadata[name] for name in new_shop_names}
        
        # 4. Generate records for the new shops
        shop_records = self.generate_shop_records(new_shops_to_add)
        
        # 5. Load the new records into the warehouse
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
        logger.error(f"DimShop transformation failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

