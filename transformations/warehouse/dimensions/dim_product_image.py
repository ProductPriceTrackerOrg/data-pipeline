"""
DimProductImage Transformation Script
Extracts and transforms product images from staging to warehouse with robust daily processing logic.
Includes duplicate prevention and proper sort order handling. The `scraped_date` field
represents the date the image was first discovered and is never updated for existing images.
"""

import json
import logging
from datetime import datetime, date, timezone
from typing import List, Dict, Optional, Tuple
from google.cloud import bigquery
from pydantic import BaseModel, ValidationError, field_validator, HttpUrl
import xxhash  # Add xxhash import

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class ProductImageModel(BaseModel):
    """Validated model for DimProductImage warehouse table."""
    image_id: int
    shop_product_id: int  # xxhash 32-bit integer
    image_url: str  # Using str instead of HttpUrl to allow custom validation
    sort_order: int
    scraped_date: date
    
    @field_validator('image_url')
    @classmethod
    def validate_image_url(cls, v):
        """
        Validate that image URL meets our requirements:
        - Must start with https://
        """
        if not v.startswith("https://"):
            raise ValueError("Image URL must start with https://")
        return v
    
    class Config:
        extra = "forbid"

class DimProductImageTransformer:
    """Transformer for DimProductImage table with daily processing logic"""
    
    def __init__(self):
        self.client = bigquery.Client(project="price-pulse-470211")
        self.project_id = "price-pulse-470211"
        self.staging_dataset = "staging"
        self.warehouse_dataset = "warehouse"
        self.table_name = "DimProductImage"
        
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> int:
        """Generate deterministic shop_product_id using xxhash 32-bit integer"""
        business_key = f"{source_website}|{product_id_native}"
        # Generate xxhash and return as integer
        hash_id = xxhash.xxh32(business_key.encode('utf-8')).intdigest()
        return hash_id
    
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
        logger.info(f"Discovered {len(tables)} staging tables: {tables}")
        return tables

    def get_existing_image_ids(self) -> set:
        """Get existing image_ids to avoid duplicates"""
        query = f"""
        SELECT DISTINCT CONCAT(CAST(shop_product_id AS STRING), '|', image_url) as image_key
        FROM `{self.project_id}.{self.warehouse_dataset}.{self.table_name}`
        """
        
        try:
            results = self.client.query(query).result()
            existing_keys = {row.image_key for row in results}
            logger.info(f"Found {len(existing_keys)} existing image keys in {self.table_name} table")
            return existing_keys
        except Exception as e:
            logger.error(f"Error getting existing image keys: {e}")
            return set()
    
    def get_next_image_id(self) -> int:
        """Get the next available image_id"""
        query = f"""
        SELECT COALESCE(MAX(image_id), 0) + 1 as next_id
        FROM `{self.project_id}.{self.warehouse_dataset}.{self.table_name}`
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            next_id = result.next_id
            logger.info(f"Next image_id will start from: {next_id}")
            return next_id
        except Exception as e:
            # If table doesn't exist, start from 1
            logger.warning(f"Could not get next image_id (table might be empty): {e}. Starting from 1.")
            return 1
    
    def is_valid_image_url(self, url: str) -> bool:
        """
        Validate if the URL is a valid image URL
        
        Validation criteria:
        1. Must start with "https://"
        2. Must be a non-empty string
        """
        if not url or not isinstance(url, str):
            return False
            
        # Check if URL starts with https://
        if not url.lower().startswith("https://"):
            logger.warning(f"Rejected invalid image URL (not https): {url[:100]}...")
            return False
            
        # Additional validations could be added here:
        # - Check for common image extensions (.jpg, .png, etc.)
        # - Check for malformed URLs
        # - Maximum URL length
        
        return True
    
    def validate_and_sort_product_images(self, product_images: List[Dict]) -> List[Dict]:
        """
        Filter out invalid images and reassign sort orders
        Ensures each product has its valid images sorted as 1,2,3...
        
        Returns only valid images with corrected sort orders
        """
        # Group images by product ID
        products = {}
        for image in product_images:
            shop_product_id = image['shop_product_id']
            if shop_product_id not in products:
                products[shop_product_id] = []
            products[shop_product_id].append(image)
        
        valid_images = []
        products_with_no_valid_images = 0
        products_with_reordered_images = 0
        
        for shop_product_id, images in products.items():
            # Filter valid images
            valid_product_images = [
                img for img in images 
                if self.is_valid_image_url(img['image_url'])
            ]
            
            if not valid_product_images:
                products_with_no_valid_images += 1
                logger.warning(f"Product {shop_product_id} has no valid images")
                continue
                
            # If the original sort order was changed, log it
            if len(valid_product_images) != len(images):
                products_with_reordered_images += 1
                
            # Reassign sort orders (1,2,3...) to valid images only
            for new_sort_order, image in enumerate(valid_product_images, 1):
                image['sort_order'] = new_sort_order
                valid_images.append(image)
        
        logger.info(f"Image validation results:")
        logger.info(f"  - Total products processed: {len(products)}")
        logger.info(f"  - Products with no valid images: {products_with_no_valid_images}")
        logger.info(f"  - Products with reordered images: {products_with_reordered_images}")
        logger.info(f"  - Valid images after filtering: {len(valid_images)}")
        
        return valid_images

    def extract_images_from_staging(self, table_name: str, target_date: str) -> List[Dict]:
        """Extract product images from staging table for a specific date"""
        
        query = f"""
        SELECT 
            raw_json_data,
            source_website,
            scrape_date
        FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
        WHERE scrape_date = '{target_date}'
        """
        
        try:
            results = self.client.query(query).result()
            raw_images = []
            
            for row in results:
                try:
                    if isinstance(row.raw_json_data, str):
                        json_data = json.loads(row.raw_json_data)
                    else:
                        json_data = row.raw_json_data
                        
                    # Sometimes json_data itself might be a string that needs parsing again (double-encoded)
                    if isinstance(json_data, str):
                        json_data = json.loads(json_data)
                    
                    products_to_process = json_data if isinstance(json_data, list) else [json_data]
                    
                    for product_data in products_to_process:
                        if not isinstance(product_data, dict):
                            logger.warning(f"Expected product data to be a dictionary, got {type(product_data)}: {str(product_data)[:100]}...")
                            continue
                            
                        product_id_native = product_data.get('product_id_native')
                        image_urls = product_data.get('image_urls', [])
                        
                        if not product_id_native or not image_urls:
                            continue
                            
                        shop_product_id = self.generate_shop_product_id(
                            row.source_website, 
                            product_id_native
                        )
                        
                        for sort_order, image_url in enumerate(image_urls, 1):
                            if image_url:
                                raw_images.append({
                                    'shop_product_id': shop_product_id,
                                    'image_url': image_url,
                                    'sort_order': sort_order
                                })
                                
                except Exception as e:
                    logger.warning(f"Error parsing product data: {e}")
                    continue
            
            logger.info(f"Extracted {len(raw_images)} raw images from {table_name}")
            
            # Apply validation and sort ordering
            valid_images = self.validate_and_sort_product_images(raw_images)
            
            logger.info(f"Validation complete - {len(valid_images)} of {len(raw_images)} images are valid")
            return valid_images
            
        except Exception as e:
            logger.error(f"Error extracting images from {table_name}: {e}")
            return []
    
    def process_images_batch(self, raw_images: List[Dict], target_date: date, starting_image_id: int) -> Tuple[List[Dict], List[Dict]]:
        """Process and validate a batch of images"""
        successful_images = []
        failed_images = []
        current_image_id = starting_image_id
        
        for raw_image in raw_images:
            try:
                # Ensure the image URL is HTTPS (double-check, even though we should have filtered these already)
                image_url = raw_image['image_url']
                if not self.is_valid_image_url(image_url):
                    logger.warning(f"Skipping image with invalid URL format: {image_url[:100]}...")
                    failed_images.append({
                        'raw_data': raw_image, 
                        'error': 'Invalid image URL: must start with https://'
                    })
                    continue
                
                image_data = {
                    'image_id': current_image_id,
                    'shop_product_id': raw_image['shop_product_id'],
                    'image_url': image_url,
                    'sort_order': raw_image['sort_order'],
                    'scraped_date': target_date
                }
                
                validated_image = ProductImageModel(**image_data)
                
                image_dict = validated_image.model_dump()
                image_dict['scraped_date'] = image_dict['scraped_date'].isoformat()
                successful_images.append(image_dict)
                current_image_id += 1
                
            except ValidationError as e:
                logger.warning(f"Validation failed for image: {e}")
                failed_images.append({'raw_data': raw_image, 'error': str(e)})
            except Exception as e:
                logger.error(f"Error processing image: {e}")
                failed_images.append({'raw_data': raw_image, 'error': str(e)})
        
        # Log validation results
        logger.info(f"Image batch processing results:")
        logger.info(f"  - Images successfully processed: {len(successful_images)}")
        logger.info(f"  - Images failed validation: {len(failed_images)}")
        
        return successful_images, failed_images
    
    def deduplicate_images(self, images: List[Dict], existing_keys: set) -> Tuple[List[Dict], int]:
        """Remove duplicate images based on shop_product_id + image_url"""
        new_images = []
        duplicate_count = 0
        
        for image in images:
            image_key = f"{image['shop_product_id']}|{image['image_url']}"
            
            if image_key not in existing_keys:
                new_images.append(image)
                existing_keys.add(image_key)
            else:
                duplicate_count += 1
        
        logger.info(f"Deduplication results:")
        logger.info(f"  - Total processed images: {len(images)}")
        logger.info(f"  - Already exist in BigQuery: {duplicate_count}")
        logger.info(f"  - New unique images to load: {len(new_images)}")
        
        return new_images, duplicate_count
    
    def load_images_to_bigquery(self, images: List[Dict]) -> bool:
        """
        Loads only NEW images to BigQuery using an append-only operation.
        This ensures that the `scraped_date` for existing images is never updated.
        """
        if not images:
            logger.info("✅ No new images to load")
            return True
        
        try:
            table_id = f"{self.project_id}.{self.warehouse_dataset}.{self.table_name}"
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("image_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("shop_product_id", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("image_url", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("sort_order", "INTEGER", mode="REQUIRED"),
                    bigquery.SchemaField("scraped_date", "DATE", mode="REQUIRED"),
                ]
            )

            logger.info(f"📤 Loading {len(images)} unique images to BigQuery...")
            load_job = self.client.load_table_from_json(images, table_id, job_config=job_config)
            load_job.result() 

            if load_job.errors:
                logger.error(f"Load job errors: {load_job.errors}")
                return False

            logger.info(f"Successfully loaded {len(images)} images to {self.table_name}")
            return True

        except Exception as e:
            logger.error(f"Error loading images to BigQuery: {e}")
            return False
    
    def transform_and_load(self, target_date: str = None):
        """Run the complete DimProductImage transformation"""
        if target_date is None:
            target_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        
        print(f"🚀 Starting DimProductImage transformation for {target_date}")
        
        try:
            print("🔍 Checking for existing images in BigQuery...")
            existing_keys = self.get_existing_image_ids()
            starting_image_id = self.get_next_image_id()
            
            staging_tables = self.get_all_staging_tables()
            
            all_successful_images = []
            all_failed_images = []
            
            # Track validation metrics
            total_raw_images_count = 0
            total_invalid_images_count = 0
            
            for table_name in staging_tables:
                print(f"\n📋 Processing {table_name}...")
                
                # Track the number of raw images before validation
                raw_count_query = f"""
                SELECT COUNT(*) as raw_count
                FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE scrape_date = '{target_date}'
                AND JSON_EXTRACT_ARRAY(raw_json_data, '$.image_urls') IS NOT NULL
                """
                
                try:
                    raw_result = list(self.client.query(raw_count_query).result())
                    if raw_result:
                        estimated_raw_count = raw_result[0].raw_count
                        total_raw_images_count += estimated_raw_count
                except Exception:
                    # If this fails, we'll still continue with the process
                    pass
                
                # Extract images with validation
                valid_images = self.extract_images_from_staging(table_name, target_date)
                
                if valid_images:
                    batch_start_id = starting_image_id + len(all_successful_images)
                    
                    successful, failed = self.process_images_batch(
                        valid_images, target_date_obj, batch_start_id
                    )
                    
                    all_successful_images.extend(successful)
                    all_failed_images.extend(failed)
                    
                    print(f"  ✅ Transformed {len(successful)} valid images")
                else:
                    print(f"  ⚠️ No valid images found in this table for the target date")
            
            # Estimate invalid images (this is approximate since we don't track exact counts)
            total_invalid_images_count = total_raw_images_count - len(all_successful_images)
            if total_invalid_images_count < 0:
                total_invalid_images_count = 0  # Guard against negative values
            
            new_images = []
            duplicate_count = 0
            if all_successful_images:
                print(f"\n🔄 Deduplicating {len(all_successful_images)} validated images against existing data...")
                new_images, duplicate_count = self.deduplicate_images(all_successful_images, existing_keys)
                
                if new_images:
                    success = self.load_images_to_bigquery(new_images)
                    if not success:
                        raise Exception("Failed to load images to BigQuery")
                else:
                    logger.info("✅ No new unique images to load after deduplication.")
            
            print("\n✅ DimProductImage transformation completed successfully!")
            print("📊 SUMMARY:")
            print(f"  - Staging tables processed: {len(staging_tables)}")
            print(f"  - Estimated raw images in staging data: {total_raw_images_count}")
            print(f"  - Images rejected by validation (not HTTPS): ~{total_invalid_images_count}")
            print(f"  - Valid images after HTTPS validation: {len(all_successful_images)}")
            print(f"  - Existing images skipped (scraped_date untouched): {duplicate_count}")
            print(f"  - New unique images loaded (scraped_date set to today): {len(new_images)}")
            print(f"  - Failed validation/processing: {len(all_failed_images)}")
            
            # Log invalid images for debugging if there are any
            if all_failed_images:
                self.log_invalid_images(all_failed_images, f"invalid_images_{target_date}.json")
                print(f"  - Invalid images logged to invalid_images_{target_date}.json for review")
            
        except Exception as e:
            logger.error(f"DimProductImage transformation failed: {e}")
            raise

    def log_invalid_images(self, invalid_images: List[Dict], output_path: str = "invalid_images.json"):
        """
        Log invalid images to a JSON file for debugging purposes
        
        Args:
            invalid_images: List of invalid images with error information
            output_path: File path to save the invalid images
        """
        try:
            if invalid_images:
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(invalid_images, f, indent=2, default=str)
                logger.info(f"Logged {len(invalid_images)} invalid images to {output_path}")
        except Exception as e:
            logger.error(f"Error logging invalid images: {e}")

def main():
    """Main execution function for DimProductImage transformation."""
    try:
        transformer = DimProductImageTransformer()
        transformer.transform_and_load()
        
    except Exception as e:
        logger.error(f"DimProductImage transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()

