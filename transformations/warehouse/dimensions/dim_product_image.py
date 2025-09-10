"""
DimProductImage Transformation Script
Extracts and transforms product images from staging to warehouse with robust daily processing logic.
Includes duplicate prevention and proper sort order handling.
"""

import hashlib
import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from google.cloud import bigquery
from pydantic import BaseModel, ValidationError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pydantic Models
class ProductImageModel(BaseModel):
    """Validated model for DimProductImage warehouse table."""
    image_id: int
    shop_product_id: str  # MD5 hash
    image_url: str
    sort_order: int
    scraped_date: date
    
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
        
        # Source to shop_id mapping
        self.shop_lookup = {
            "appleme.lk": 1,
            "simplytek.lk": 2,
            "onei.lk": 3
        }
    
    def generate_shop_product_id(self, source_website: str, product_id_native: str) -> str:
        """Generate deterministic shop_product_id using MD5 hash"""
        business_key = f"{source_website}|{product_id_native}"
        md5_hash = hashlib.md5(business_key.encode('utf-8')).hexdigest()
        return md5_hash
    
    def get_existing_image_ids(self) -> set:
        """Get existing image_ids to avoid duplicates"""
        query = f"""
        SELECT DISTINCT CONCAT(shop_product_id, '|', image_url) as image_key
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
        SELECT COALESCE(MAX(image_id), -1) + 1 as next_id
        FROM `{self.project_id}.{self.warehouse_dataset}.{self.table_name}`
        """
        
        try:
            result = list(self.client.query(query).result())[0]
            next_id = result.next_id
            logger.info(f"Next image_id will start from: {next_id}")
            return next_id
        except Exception as e:
            logger.error(f"Error getting next image_id: {e}")
            return 0
    
    def extract_images_from_staging(self, table_name: str, target_date: str) -> List[Dict]:
        """Extract product images from staging table for a specific date"""
        query = f"""
        SELECT 
            JSON_VALUE(product, '$.product_id_native') as product_id_native,
            source_website,
            JSON_QUERY_ARRAY(product, '$.image_urls') as image_urls
        FROM `{self.project_id}.{self.staging_dataset}.{table_name}`,
        UNNEST(JSON_QUERY_ARRAY(raw_json_data)) AS product
        WHERE scrape_date = '{target_date}'
        AND JSON_QUERY_ARRAY(product, '$.image_urls') IS NOT NULL
        AND ARRAY_LENGTH(JSON_QUERY_ARRAY(product, '$.image_urls')) > 0
        """
        
        try:
            results = self.client.query(query).result()
            images = []
            
            for row in results:
                # Parse image URLs array
                if row.image_urls:
                    import json
                    image_urls = json.loads(row.image_urls) if isinstance(row.image_urls, str) else row.image_urls
                    
                    # Generate shop_product_id
                    shop_product_id = self.generate_shop_product_id(
                        row.source_website, 
                        row.product_id_native
                    )
                    
                    # Create image records with sort_order
                    for sort_order, image_url in enumerate(image_urls, 1):
                        if image_url:  # Skip empty URLs
                            images.append({
                                'shop_product_id': shop_product_id,
                                'image_url': image_url.strip(),
                                'sort_order': sort_order,
                                'source_website': row.source_website,
                                'product_id_native': row.product_id_native
                            })
            
            logger.info(f"Extracted {len(images)} images from {table_name}")
            return images
            
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
                # Create validated image record
                image_data = {
                    'image_id': current_image_id,
                    'shop_product_id': raw_image['shop_product_id'],
                    'image_url': raw_image['image_url'],
                    'sort_order': raw_image['sort_order'],
                    'scraped_date': target_date  # Keep as date object for Pydantic
                }
                
                # Validate with Pydantic
                validated_image = ProductImageModel(**image_data)
                
                # Convert to dict and handle date serialization
                image_dict = validated_image.model_dump()
                image_dict['scraped_date'] = target_date.strftime('%Y-%m-%d')  # Convert date to string for BigQuery
                successful_images.append(image_dict)
                current_image_id += 1
                
            except ValidationError as e:
                logger.warning(f"Validation failed for image: {e}")
                failed_images.append({
                    'raw_data': raw_image,
                    'error': str(e)
                })
            except Exception as e:
                logger.error(f"Error processing image: {e}")
                failed_images.append({
                    'raw_data': raw_image,
                    'error': str(e)
                })
        
        return successful_images, failed_images
    
    def deduplicate_images(self, images: List[Dict], existing_keys: set) -> Tuple[List[Dict], int]:
        """Remove duplicate images based on shop_product_id + image_url"""
        new_images = []
        duplicate_count = 0
        
        for image in images:
            image_key = f"{image['shop_product_id']}|{image['image_url']}"
            
            if image_key not in existing_keys:
                new_images.append(image)
            else:
                duplicate_count += 1
        
        logger.info(f"Deduplication results:")
        logger.info(f"  - Total input images: {len(images)}")
        logger.info(f"  - Already exist in BigQuery: {duplicate_count}")
        logger.info(f"  - New unique images: {len(new_images)}")
        
        return new_images, duplicate_count
    
    def load_images_to_bigquery(self, images: List[Dict]) -> bool:
        """Load new images to BigQuery with daily processing logic"""
        if not images:
            logger.info("✅ No new images to load")
            return True

        # Deduplicate images in-memory before loading
        unique_images = {}
        for img in images:
            key = f"{img['shop_product_id']}|{img['image_url']}"
            # Only keep the first occurrence (lowest image_id)
            if key not in unique_images or img['image_id'] < unique_images[key]['image_id']:
                unique_images[key] = img

        deduped_images = list(unique_images.values())
        logger.info(f"🧹 Deduplicated batch: {len(images)} → {len(deduped_images)} unique images")

        try:
            table_id = f"{self.project_id}.{self.warehouse_dataset}.{self.table_name}"

            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )

            # Load data
            logger.info(f"📤 Loading {len(deduped_images)} unique images to BigQuery...")
            # Convert date to string for JSON serialization
            for img in deduped_images:
                if isinstance(img.get('scraped_date'), date):
                    img['scraped_date'] = img['scraped_date'].isoformat()

            load_job = self.client.load_table_from_json(
                deduped_images,
                table_id,
                job_config=job_config
            )

            load_job.result()  # Wait for completion

            if load_job.errors:
                logger.error(f"Load job errors: {load_job.errors}")
                return False

            logger.info(f"Successfully loaded {len(deduped_images)} images to {self.table_name}")
            return True

        except Exception as e:
            logger.error(f"Error loading images to BigQuery: {e}")
            return False
    
    def transform_and_load(self, target_date: str = None):
        """Run the complete DimProductImage transformation with daily processing logic"""
        if target_date is None:
            target_date = date.today().strftime("%Y-%m-%d")
        
        target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
        
        print(f"🚀 Starting DimProductImage transformation for {target_date}")
        
        try:
            # Get existing image keys to avoid duplicates
            print("🔍 Checking for existing images in BigQuery...")
            existing_keys = self.get_existing_image_ids()
            
            # Get next image_id
            starting_image_id = self.get_next_image_id()
            
            # Discover staging tables
            staging_tables = ['stg_raw_appleme', 'stg_raw_simplytek', 'stg_raw_onei_lk']
            logger.info(f"Discovered {len(staging_tables)} staging tables: {staging_tables}")
            
            all_images = []
            all_failed = []
            
            # Process each staging table
            for table_name in staging_tables:
                print(f"\n📋 Processing {table_name}...")
                
                # Extract images from staging
                raw_images = self.extract_images_from_staging(table_name, target_date)
                
                if raw_images:
                    # Process images
                    successful_images, failed_images = self.process_images_batch(
                        raw_images, target_date_obj, starting_image_id + len(all_images)
                    )
                    
                    all_images.extend(successful_images)
                    all_failed.extend(failed_images)
                    
                    print(f"  ✅ Transformed {len(successful_images)} images")
                else:
                    print(f"  ⚠️ No images found")
            
            # Deduplicate against existing data
            if all_images:
                print(f"\n🔄 Deduplicating {len(all_images)} images against existing data...")
                new_images, duplicate_count = self.deduplicate_images(all_images, existing_keys)
                
                # Load new images only
                if new_images:
                    success = self.load_images_to_bigquery(new_images)
                    if not success:
                        raise Exception("Failed to load images to BigQuery")
                else:
                    logger.info("✅ No new images to load - all images already exist in BigQuery")
            else:
                new_images = []
                duplicate_count = 0
            
            # Summary
            print("✅ DimProductImage transformation completed successfully!")
            print("📊 SUMMARY:")
            print(f"  - Staging tables processed: {len(staging_tables)}")
            print(f"  - Total images extracted: {len(all_images)}")
            print(f"  - Existing images in BigQuery: {len(existing_keys)}")
            print(f"  - New unique images loaded: {len(new_images) if new_images else 0}")
            print(f"  - Failed images: {len(all_failed)}")
            
            if all_failed:
                logger.warning(f"Failed to process {len(all_failed)} images")
                for failed in all_failed[:5]:  # Show first 5
                    logger.warning(f"  - {failed['error']}")
            
            logger.info("DimProductImage transformation completed successfully")
            
        except Exception as e:
            logger.error(f"DimProductImage transformation failed: {e}")
            raise

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
