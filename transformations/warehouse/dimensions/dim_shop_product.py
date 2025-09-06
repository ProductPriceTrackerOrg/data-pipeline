"""
DimShopProduct Transformation Script
Transforms raw product data from staging to warehouse with robust error handling,
HTML cleaning, Pydantic validation, and fallback strategies.
"""

import json
import re
import html
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
import logging
from bs4 import BeautifulSoup
from pydantic import BaseModel, ValidationError, validator, Field
from google.cloud import bigquery
from ..utils.transformation_utils import TransformationBase

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class RawProductData(BaseModel):
    """Model for raw product data from staging JSON."""
    product_id_native: str
    product_url: str
    product_title: str
    description_html: Optional[str] = None
    brand: Optional[str] = None
    category_path: Optional[List[str]] = None
    image_urls: Optional[List[str]] = None
    variants: Optional[List[Dict]] = None
    metadata: Dict
    
    class Config:
        extra = "allow"  # Allow extra fields in raw data

class ShopProductModel(BaseModel):
    """Validated model for DimShopProduct warehouse table."""
    shop_product_id: str  # MD5 hash string
    shop_id: int
    product_title_native: str = Field(..., max_length=500)
    brand_native: Optional[str] = Field(None, max_length=100)
    description_native: Optional[str] = Field(None, max_length=2000)
    product_url: Optional[str] = Field(None, max_length=1024)
    scraped_date: date
    predicted_master_category_id: Optional[int] = None
    
    @validator('product_title_native')
    def validate_title(cls, v):
        if not v or not v.strip():
            raise ValueError('Product title cannot be empty')
        return v.strip()[:500]  # Enforce length limit
    
    @validator('product_url')
    def validate_url(cls, v):
        if v and not (v.startswith('http://') or v.startswith('https://')):
            return f"https://{v}"  # Add https if missing
        return v
    
    @validator('shop_id')
    def validate_shop_id(cls, v):
        if v < 0:
            raise ValueError('Shop ID must be non-negative')
        return v
    
    @validator('brand_native', pre=True)
    def clean_brand(cls, v):
        if v:
            return str(v).strip()[:100]
        return None
    
    @validator('description_native', pre=True)
    def clean_and_validate_description(cls, v):
        """Clean HTML and validate description with fallbacks."""
        if not v:
            return None
            
        try:
            # Primary cleaning with BeautifulSoup
            cleaned = clean_html_description(v)
            if cleaned and len(cleaned.strip()) > 0:
                return cleaned[:2000]
        except Exception as e:
            logger.debug(f"Primary HTML cleaning failed: {e}")
        
        try:
            # Fallback: basic regex cleaning
            cleaned = re.sub(r'<[^>]+>', '', str(v))
            cleaned = html.unescape(cleaned)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            if cleaned:
                return cleaned[:2000]
        except Exception as e:
            logger.debug(f"Fallback HTML cleaning failed: {e}")
        
        # Return None if all cleaning fails
        return None

# HTML Cleaning Functions
def clean_html_description(html_text: str) -> Optional[str]:
    """
    Clean HTML description perfectly using BeautifulSoup.
    
    Args:
        html_text: Raw HTML text to clean
        
    Returns:
        Clean text without HTML tags
    """
    if not html_text:
        return None
        
    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')
        
        # Extract clean text
        clean_text = soup.get_text()
        
        # Clean up whitespace and formatting
        clean_text = re.sub(r'\s+', ' ', clean_text)  # Multiple spaces → single space
        clean_text = re.sub(r'\n+', ' ', clean_text)  # Multiple newlines → single space
        clean_text = html.unescape(clean_text)        # Decode HTML entities
        clean_text = clean_text.strip()               # Remove leading/trailing spaces
        
        return clean_text if clean_text else None
        
    except Exception as e:
        logger.debug(f"BeautifulSoup cleaning failed: {e}")
        # Fallback to manual cleaning
        return manual_html_strip(html_text)

def manual_html_strip(html_text: str) -> Optional[str]:
    """
    Fallback method using regex for extreme edge cases.
    
    Args:
        html_text: Raw HTML text to clean
        
    Returns:
        Clean text without HTML tags
    """
    try:
        # Remove all HTML tags
        clean_text = re.sub(r'<[^>]+>', '', html_text)
        
        # Decode HTML entities
        clean_text = html.unescape(clean_text)
        
        # Clean whitespace
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        
        return clean_text if clean_text else None
    except Exception as e:
        logger.debug(f"Manual HTML stripping failed: {e}")
        return None

# Main Transformation Class
class DimShopProductTransformer(TransformationBase):
    """
    Handles the transformation and loading of shop product dimension data.
    """
    
    def __init__(self, project_id: str = "price-pulse-470211"):
        """
        Initialize the DimShopProduct transformer.
        
        Args:
            project_id: Google Cloud project ID
        """
        super().__init__(project_id)
        self.table_name = "DimShopProduct"
        self.shop_lookup = {}  # Cache for shop_id lookups
        
    def build_shop_lookup(self) -> Dict[str, int]:
        """
        Build lookup table for shop_name -> shop_id mapping.
        
        Returns:
            Dictionary mapping shop names to shop IDs
        """
        try:
            query = f"""
            SELECT shop_name, shop_id
            FROM `{self.get_table_ref('DimShop')}`
            """
            
            results = self.client.query(query).result()
            shop_lookup = {}
            
            for row in results:
                # Handle different possible shop name formats
                shop_lookup[row.shop_name] = row.shop_id
                
                # Add variations (with/without www, with/without https)
                if row.shop_name.startswith('www.'):
                    shop_lookup[row.shop_name[4:]] = row.shop_id
                else:
                    shop_lookup[f"www.{row.shop_name}"] = row.shop_id
            
            logger.info(f"Built shop lookup with {len(shop_lookup)} entries")
            self.shop_lookup = shop_lookup
            return shop_lookup
            
        except Exception as e:
            logger.error(f"Failed to build shop lookup: {e}")
            return {}
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> str:
        """
        Generate a unique, deterministic shop_product_id using MD5 hash of business key.
        
        Args:
            source_website: Source website name
            product_id_native: Native product ID from the website
            
        Returns:
            Hexadecimal string representing the shop_product_id
        """
        import hashlib
        
        # Create business key: source_website|product_id_native
        business_key = f"{source_website}|{product_id_native}"
        
        # Generate MD5 hash and convert to hex
        md5_hash = hashlib.md5(business_key.encode('utf-8')).hexdigest()
        
        return md5_hash
    
    def get_all_staging_tables(self) -> List[str]:
        """
        Dynamically discover all staging tables that contain raw product data.
        
        Returns:
            List of staging table names
        """
        try:
            # Query to find all tables in the staging dataset that start with 'stg_raw_'
            query = f"""
            SELECT table_name
            FROM `{self.project_id}.{self.staging_dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name LIKE 'stg_raw_%'
            ORDER BY table_name
            """
            
            results = self.client.query(query).result()
            staging_tables = [row.table_name for row in results]
            
            logger.info(f"Discovered {len(staging_tables)} staging tables: {staging_tables}")
            return staging_tables
            
        except Exception as e:
            logger.warning(f"Could not discover staging tables dynamically: {e}")
            # Fallback to known tables if discovery fails
            fallback_tables = ["stg_raw_simplytek", "stg_raw_appleme", "stg_raw_onei_lk"]
            logger.info(f"Using fallback tables: {fallback_tables}")
            return fallback_tables

    def extract_products_from_staging(self, target_date: date = None) -> List[Dict]:
        """
        Extract product data from all staging tables (dynamically discovered).
        
        Args:
            target_date: Date to extract data for (defaults to today)
            
        Returns:
            List of raw product JSON data
        """
        if target_date is None:
            target_date = datetime.now().date()
            
        all_products = []
        
        # Dynamically discover all staging tables
        staging_tables = self.get_all_staging_tables()
        
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
                
                # Extract data from this table
                query = f"""
                SELECT 
                    raw_json_data,
                    source_website,
                    scrape_date
                FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE scrape_date = '{target_date}'
                """
                
                results = self.client.query(query).result()
                products_from_table = 0
                
                for row in results:
                    try:
                        # Parse JSON data
                        if isinstance(row.raw_json_data, str):
                            json_data = json.loads(row.raw_json_data)
                        else:
                            json_data = row.raw_json_data
                        
                        # Handle JSON array of products (each row contains multiple products)
                        if isinstance(json_data, list):
                            for single_product in json_data:
                                # Ensure metadata exists for each product
                                if 'metadata' not in single_product:
                                    single_product['metadata'] = {}
                                
                                # Add staging metadata to each individual product
                                single_product['metadata']['source_website'] = row.source_website
                                single_product['metadata']['scrape_date'] = str(row.scrape_date)
                                
                                all_products.append(single_product)
                                products_from_table += 1
                        else:
                            # Handle single product object (legacy format)
                            if 'metadata' not in json_data:
                                json_data['metadata'] = {}
                            
                            json_data['metadata']['source_website'] = row.source_website
                            json_data['metadata']['scrape_date'] = str(row.scrape_date)
                            
                            all_products.append(json_data)
                            products_from_table += 1
                        
                    except Exception as e:
                        logger.warning(f"Failed to parse JSON from {table_name}: {e}")
                        continue
                        
                logger.info(f"Extracted {products_from_table} products from {table_name}")
                
            except Exception as e:
                logger.warning(f"Could not extract products from {table_name}: {e}")
                continue
                
        logger.info(f"Total products extracted: {len(all_products)}")
        return all_products
    
    def transform_single_product(
        self, 
        raw_json: Dict, 
        scrape_date: date
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Transform a single raw product to validated shop product.
        
        Args:
            raw_json: Raw product JSON data
            scrape_date: Date the product was scraped
            
        Returns:
            Tuple of (validated_product_dict, error_message)
        """
        try:
            # Parse raw data with Pydantic
            raw_product = RawProductData(**raw_json)
            
            # Lookup shop_id
            source_website = raw_product.metadata.get('source_website', '')
            shop_id = self.shop_lookup.get(source_website)
            
            if shop_id is None:
                # Try variations
                for shop_name, shop_id_val in self.shop_lookup.items():
                    if source_website in shop_name or shop_name in source_website:
                        shop_id = shop_id_val
                        break
                        
                if shop_id is None:
                    return None, f"Shop ID not found for: {source_website}"
            
            # Generate deterministic shop_product_id using MD5 hash
            shop_product_id = self.generate_shop_product_id(
                source_website, 
                raw_product.product_id_native
            )
            
            # Create validated shop product
            shop_product = ShopProductModel(
                shop_product_id=shop_product_id,
                shop_id=shop_id,
                product_title_native=raw_product.product_title,
                brand_native=raw_product.brand,
                description_native=raw_product.description_html,  # Pydantic handles cleaning
                product_url=raw_product.product_url,
                scraped_date=scrape_date,
                predicted_master_category_id=None
            )
            
            # Convert to dict with proper date serialization
            result_dict = shop_product.model_dump()
            result_dict['scraped_date'] = str(scrape_date)  # Convert date to string for JSON
            
            return result_dict, None
            
        except ValidationError as e:
            return None, f"Validation failed: {e}"
        except Exception as e:
            return None, f"Transformation failed: {e}"
    
    def deduplicate_products(self, products: List[Dict]) -> Tuple[List[Dict], int]:
        """
        Remove duplicate products based on shop_product_id, keeping the first occurrence.
        
        Args:
            products: List of product dictionaries
            
        Returns:
            Tuple of (deduplicated_products, duplicate_count)
        """
        seen_ids = set()
        deduplicated_products = []
        duplicate_count = 0
        
        for product in products:
            shop_product_id = product.get('shop_product_id')
            
            if shop_product_id not in seen_ids:
                seen_ids.add(shop_product_id)
                deduplicated_products.append(product)
            else:
                duplicate_count += 1
                logger.debug(f"Removing duplicate shop_product_id: {shop_product_id}")
        
        logger.info(f"Deduplication: {len(products)} → {len(deduplicated_products)} products ({duplicate_count} duplicates removed)")
        return deduplicated_products, duplicate_count

    def get_existing_shop_product_ids(self) -> set:
        """Get all existing shop_product_ids from DimShopProduct to avoid duplicates"""
        try:
            query = f"""
            SELECT DISTINCT shop_product_id 
            FROM `{self.project_id}.warehouse.DimShopProduct`
            """
            
            results = list(self.client.query(query).result())
            existing_ids = {row.shop_product_id for row in results}
            
            logger.info(f"Found {len(existing_ids)} existing shop_product_ids in DimShopProduct")
            return existing_ids
            
        except Exception as e:
            logger.warning(f"Could not fetch existing shop_product_ids (table might not exist): {e}")
            return set()

    def separate_new_products(self, products: List[Dict], existing_ids: set) -> List[Dict]:
        """Filter out existing products, return only NEW products for insertion"""
        new_products = []
        existing_count = 0
        
        for product in products:
            shop_product_id = product.get('shop_product_id')
            
            if shop_product_id not in existing_ids:
                new_products.append(product)
            else:
                existing_count += 1
        
        logger.info(f"Product filtering: {len(new_products)} new products to insert, {existing_count} existing products skipped")
        return new_products

    def insert_new_products(self, new_products: List[Dict]) -> bool:
        """Insert only NEW products to DimShopProduct"""
        if not new_products:
            logger.info("No new products to insert")
            return True
        
        try:
            table_ref = self.get_table_ref(self.table_name)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("shop_product_id", "STRING", mode="REQUIRED"),  # MD5 hash
                    bigquery.SchemaField("shop_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("product_title_native", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("brand_native", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("description_native", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("product_url", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("scraped_date", "DATE", mode="NULLABLE"),
                    bigquery.SchemaField("predicted_master_category_id", "INTEGER", mode="NULLABLE"),
                ]
            )
            
            job = self.client.load_table_from_json(
                new_products, 
                table_ref, 
                job_config=job_config
            )
            job.result()
            
            logger.info(f"✅ Successfully inserted {len(new_products)} new products")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert new products: {e}")
            return False

    def update_all_products_scrape_date(self, all_products: List[Dict], target_date: date) -> bool:
        """
        Update scrape_date for ALL products (both existing and newly inserted)
        This ensures all products reflect the latest scrape_date from staging
        """
        if not all_products:
            logger.info("No products to update scrape_date")
            return True
        
        try:
            # Get all shop_product_ids from this batch
            shop_product_ids = [f"'{product['shop_product_id']}'" for product in all_products]
            
            # Process in batches to avoid query limits
            batch_size = 1000
            total_updated = 0
            
            for i in range(0, len(shop_product_ids), batch_size):
                batch_ids = shop_product_ids[i:i + batch_size]
                ids_list = ", ".join(batch_ids)
                
                # Update scrape_date for products in this batch
                update_query = f"""
                UPDATE `{self.project_id}.warehouse.DimShopProduct`
                SET scraped_date = '{target_date}'
                WHERE shop_product_id IN ({ids_list})
                """
                
                try:
                    job = self.client.query(update_query)
                    job.result()
                    total_updated += len(batch_ids)
                    
                except Exception as e:
                    logger.warning(f"Batch update failed: {e}")
                    continue
            
            logger.info(f"✅ Updated scrape_date to {target_date} for {total_updated} products")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to update scrape_date: {e}")
            return False

    def process_products_batch(
        self, 
        raw_products: List[Dict], 
        scrape_date: date
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Process batch of products with comprehensive error handling.
        
        Args:
            raw_products: List of raw product JSON data
            scrape_date: Date the products were scraped
            
        Returns:
            Tuple of (successful_products, failed_products)
        """
        successful_products = []
        failed_products = []
        
        for i, raw_json in enumerate(raw_products):
            product_data, error = self.transform_single_product(
                raw_json, scrape_date
            )
            
            if product_data:
                successful_products.append(product_data)
            else:
                failed_products.append({
                    'index': i,
                    'product_id': raw_json.get('product_id_native', 'unknown'),
                    'source_website': raw_json.get('metadata', {}).get('source_website', 'unknown'),
                    'error': error
                })
                logger.warning(f"Product {i} failed: {error}")
        
        return successful_products, failed_products
    
    def load_products_to_bigquery(self, products: List[Dict], target_date: date) -> bool:
        """
        Daily transformation process that handles existing vs new products properly:
        1. Check existing products
        2. Insert only NEW products  
        3. Update scrape_date for ALL products
        
        Args:
            products: List of validated product dictionaries
            target_date: Date being processed
            
        Returns:
            Success status
        """
        if not products:
            logger.info("No products to load")
            return True
            
        logger.info(f"🚀 Starting daily DimShopProduct load for {target_date}")
        
        # Step 1: Get existing product IDs
        logger.info("🔍 Step 1: Checking existing products in warehouse...")
        existing_ids = self.get_existing_shop_product_ids()
        
        # Step 2: Separate and insert only NEW products
        logger.info("📥 Step 2: Filtering and inserting NEW products...")
        new_products = self.separate_new_products(products, existing_ids)
        
        success = True
        if new_products:
            success &= self.insert_new_products(new_products)
        
        # Step 3: Update scrape_date for ALL products (existing + new)
        logger.info(f"🔄 Step 3: Updating scrape_date for ALL {len(products)} products...")
        success &= self.update_all_products_scrape_date(products, target_date)
        
        # Summary
        if success:
            logger.info("✅ Daily DimShopProduct load completed successfully!")
            logger.info(f"📊 LOAD SUMMARY:")
            logger.info(f"  - Total products processed: {len(products)}")
            logger.info(f"  - New products inserted: {len(new_products)}")
            logger.info(f"  - Existing products found: {len(products) - len(new_products)}")
            logger.info(f"  - Products with updated scrape_date: {len(products)}")
        else:
            logger.error("❌ Daily DimShopProduct load failed")
        
        return success
    
    def log_transformation_summary(
        self, 
        successful_count: int, 
        failed_count: int, 
        failed_products: List[Dict]
    ) -> None:
        """
        Log comprehensive transformation summary.
        
        Args:
            successful_count: Number of successful transformations
            failed_count: Number of failed transformations
            failed_products: List of failed product details
        """
        total = successful_count + failed_count
        success_rate = (successful_count / total * 100) if total > 0 else 0
        
        logger.info(f"DimShopProduct Transformation Summary:")
        logger.info(f"  Total products processed: {total}")
        logger.info(f"  Successful: {successful_count}")
        logger.info(f"  Failed: {failed_count}")
        logger.info(f"  Success rate: {success_rate:.2f}%")
        
        # Log failure breakdown
        if failed_products:
            failure_types = {}
            source_failures = {}
            
            for failed in failed_products:
                # Count error types
                error_type = failed['error'].split(':')[0]
                failure_types[error_type] = failure_types.get(error_type, 0) + 1
                
                # Count failures by source
                source = failed['source_website']
                source_failures[source] = source_failures.get(source, 0) + 1
            
            logger.info("Failure breakdown by type:")
            for error_type, count in failure_types.items():
                logger.info(f"  {error_type}: {count}")
                
            logger.info("Failure breakdown by source:")
            for source, count in source_failures.items():
                logger.info(f"  {source}: {count}")
    
    def transform_and_load(self, target_date: date = None) -> None:
        """
        Complete transformation process for DimShopProduct.
        
        Args:
            target_date: Date to process (defaults to today)
        """
        if target_date is None:
            target_date = datetime.now().date()
            
        self.log_transformation_start(f"{self.table_name} for {target_date}")
        
        try:
            # Build shop lookup
            if not self.build_shop_lookup():
                raise Exception("Failed to build shop lookup - cannot proceed")
            
            # Extract products from staging
            raw_products = self.extract_products_from_staging(target_date)
            
            if not raw_products:
                logger.info(f"No products found for {target_date}")
                return
            
            # Process products (no need for starting ID with MD5 approach)
            successful_products, failed_products = self.process_products_batch(
                raw_products, target_date
            )
            
            # Deduplicate successful products
            if successful_products:
                deduplicated_products, duplicate_count = self.deduplicate_products(successful_products)
                logger.info(f"Removed {duplicate_count} duplicate products during transformation")
            else:
                deduplicated_products = []
                duplicate_count = 0
            
            # Load deduplicated products with daily logic
            if deduplicated_products:
                success = self.load_products_to_bigquery(deduplicated_products, target_date)
                if not success:
                    raise Exception("Failed to load products to BigQuery")
            
            # Log summary (use deduplicated count)
            self.log_transformation_summary(
                len(deduplicated_products), 
                len(failed_products), 
                failed_products
            )
            
            self.log_transformation_complete(self.table_name, len(deduplicated_products))
            
        except Exception as e:
            logger.error(f"DimShopProduct transformation failed: {e}")
            raise

def main():
    """
    Main execution function for DimShopProduct transformation.
    """
    try:
        transformer = DimShopProductTransformer()
        transformer.transform_and_load()
        
    except Exception as e:
        logger.error(f"DimShopProduct transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()
