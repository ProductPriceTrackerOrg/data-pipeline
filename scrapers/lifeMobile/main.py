"""
LifeMobile Web Scraper - Fresh Data Pipeline
Scrapes product data from LifeMobile.lk and uploads to Azure Data Lake Storage (ADLS)

Usage:
    python main.py
"""

import logging
import sys
import os
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the current directory to the path to allow imports
sys.path.append('.')

# Azure Storage imports
try:
    from azure.storage.blob import BlobServiceClient
    AZURE_AVAILABLE = True
except ImportError:
    AZURE_AVAILABLE = False
    print("Azure SDK not installed. Run: pip install azure-storage-blob")

from scripts.product_scraper_manager import LifeMobileScrapingManager, setup_scraping_environment


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    LifeMobile Web Scraper                    ║
║                  Fresh Data Pipeline                         ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def clean_existing_data():
    """Remove existing JSON files to ensure fresh data"""
    json_files = [
        "lifemobile_products.json",
        "lifemobile_products_fixed.json", 
        "lifemobile_products_backup.json"
    ]
    
    cleaned_count = 0
    for file_path in json_files:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                cleaned_count += 1
                print(f"Removed: {file_path}")
            except Exception as e:
                print(f"Could not remove {file_path}: {e}")
    
    if cleaned_count > 0:
        print(f"Cleaned {cleaned_count} existing data files")
    else:
        print("No existing data files to clean")


def upload_to_adls(json_data: str, source_website: str):
    """
    Uploads a JSON string to ADLS as a single JSON file using chunked upload for large files.
    
    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
    """
    if not AZURE_AVAILABLE:
        print("Azure SDK not available. Cannot upload to ADLS.")
        return False
    
    # Get Azure Connection String from Environment Variable
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("Azure connection string not found in environment variables.")
        print("Please set AZURE_STORAGE_CONNECTION_STRING in your .env file")
        return False

    # Define the partitioned path
    utc_now = datetime.now(timezone.utc)
    scrape_date = utc_now.strftime('%Y-%m-%d')
    file_path = f"source_website={source_website}/scrape_date={scrape_date}/data.json"
    container_name = "raw-data"

    try:
        # Import required Azure modules
        from azure.storage.blob import ContentSettings, BlobBlock
        import uuid
        import base64
        
        # Convert JSON string to bytes
        data_bytes = json_data.encode('utf-8')
        file_size = len(data_bytes)
        print(f"File size: {file_size / (1024 * 1024):.2f} MB")
        
        # Configure service client with increased timeouts
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string,
            connection_timeout=60,  # Connection timeout
            read_timeout=300,       # Read timeout
            socket_timeout=300      # Socket timeout
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
        
        print(f"Uploading data to: {container_name}/{file_path}")
        print(f"Using chunked upload method for large file")
        
        # Define chunk size - 4 MB per chunk
        chunk_size = 4 * 1024 * 1024  # 4 MB chunks
        
        # Calculate number of chunks
        num_chunks = (file_size + chunk_size - 1) // chunk_size
        print(f"Splitting file into {num_chunks} chunks of {chunk_size / (1024 * 1024):.2f} MB each")
        
        # Generate block IDs
        block_ids = []
        
        # Upload chunks
        for i in range(num_chunks):
            # Calculate chunk start and end
            start = i * chunk_size
            end = min(start + chunk_size, file_size)
            
            # Get the chunk data
            chunk_data = data_bytes[start:end]
            
            # Generate unique block ID and encode it to base64
            block_id = str(uuid.uuid4())
            encoded_block_id = base64.b64encode(block_id.encode('utf-8')).decode('utf-8')
            block_ids.append(encoded_block_id)
            
            # Upload the block
            print(f"Uploading chunk {i+1}/{num_chunks} ({len(chunk_data) / (1024 * 1024):.2f} MB)...")
            blob_client.stage_block(
                block_id=encoded_block_id,
                data=chunk_data,
                timeout=60  # Shorter timeout for individual chunks
            )
            
        # Commit the blocks with proper content settings
        print(f"Committing all blocks...")
        blob_client.commit_block_list(
            block_ids,
            content_settings=ContentSettings(content_type='application/json'),
            timeout=60
        )

        print("Upload to Azure Data Lake Storage completed successfully!")
        print(f"Location: {container_name}/{file_path}")
        return True

    except Exception as e:
        print(f"ADLS upload error: {e}")
        logging.error(f"ADLS upload error: {e}", exc_info=True)
        return False


def load_scraped_products(json_path="lifemobile_products.json"):
    """
    Load scraped products from the local JSON file with comprehensive error handling
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"{json_path} not found. Scraper did not produce output.")
    
    try:
        # First try normal JSON loading
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        # Validate data structure
        if not isinstance(data, list):
            raise ValueError(f"Expected list of products, got {type(data)}")
            
        # Check if items are dictionaries (not strings)
        dict_count = sum(1 for item in data if isinstance(item, dict))
        string_count = sum(1 for item in data if isinstance(item, str))
        
        print(f"Data type check: {type(data)}")
        if len(data) > 0:
            print(f"First item type: {type(data[0])}")
            if isinstance(data[0], str):
                print(f"First item preview: {data[0][:50]}...")

        if string_count > 0:
            print(f"Data contains strings instead of objects, attempting to parse...")
            # Try to parse string items as JSON
            fixed_data = []
            for item in data:
                if isinstance(item, str):
                    try:
                        parsed_item = json.loads(item)
                        fixed_data.append(parsed_item)
                    except json.JSONDecodeError:
                        continue  # Skip invalid items
                elif isinstance(item, dict):
                    fixed_data.append(item)
            
            if len(fixed_data) > 0:
                print(f"Successfully parsed {len(fixed_data)} valid products from {len(data)} total items")
                return fixed_data
            else:
                raise ValueError("Failed to parse string data: No valid JSON objects found")

        print(f"Data quality check passed: {dict_count} dictionary objects, {string_count} string objects")
        return data
        
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        raise Exception(f"Unable to parse JSON file: {e}")


def main():
    """Main entry point - scrape fresh products and upload to Azure"""
    print_banner()
    print("Starting LifeMobile fresh data pipeline...")
    print(f"Pipeline started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Setup environment and logging
        setup_scraping_environment()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('lifemobile_scraper.log'),
                logging.StreamHandler()
            ]
        )
        
        # Step 0: Clean existing data files
        print("\nStep 0: Cleaning existing data files...")
        clean_existing_data()
        
        # Step 1: Run fresh scraping
        print("\nStep 1: Scraping fresh products from LifeMobile.lk...")
        start_time = datetime.now(timezone.utc)
        
        manager = LifeMobileScrapingManager()
        scraping_success = manager.run_scraper()
        
        if not scraping_success:
            print("Scraping failed!")
            return 1
        
        scraping_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        print(f"Scraping completed in {scraping_duration:.2f} seconds")

        # Step 2: Load scraped products and remove JSON file immediately
        print("\nStep 2: Loading scraped data...")
        products_data = load_scraped_products("lifemobile_products.json")
        
        # # Immediately remove the JSON file after loading
        # try:
        #     if os.path.exists("lifemobile_products.json"):
        #         os.remove("lifemobile_products.json")
        #         print("Removed temporary JSON file from directory")
        # except Exception as e:
        #     print(f"Could not remove JSON file: {e}")

        # if not products_data:
        #     print("No valid products found in scraped data")
        #     return 1
        
        # Step 3: Data quality validation
        print("\nStep 3: Data quality validation...")
        total_products = len(products_data)
        total_variants = sum(len(product.get('variants', [])) for product in products_data if isinstance(product, dict))
        scraping_speed = total_products / scraping_duration if scraping_duration > 0 else 0
        
        # Validate data quality
        dict_objects = sum(1 for item in products_data if isinstance(item, dict))
        string_objects = sum(1 for item in products_data if isinstance(item, str))

        print(f"Data Quality Check:")
        print(f"   Dictionary objects: {dict_objects}")
        print(f"   String objects: {string_objects}")
        print(f"   Scraping speed: {scraping_speed:.2f} products/second")
        
        if string_objects > 0:
            print("Data quality check failed - contains string objects")
            print("Please fix the data format before uploading")
            return 1
        else:
            print("Data quality check passed - all objects are properly formatted")
        
        # Display scraping results
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"Products scraped: {total_products:,}")
        print(f"Variants scraped: {total_variants:,}")
        print(f"Scraping time: {scraping_duration:.2f} seconds")
        print(f"Scraping speed: {scraping_speed:.2f} products/sec")

        # Step 4: Upload data to Azure Data Lake Storage
        if AZURE_AVAILABLE:
            try:
                print(f"\n{'='*60}")
                print(f"UPLOADING TO AZURE DATA LAKE STORAGE")
                print(f"{'='*60}")
                
                # Handle datetime serialization
                def datetime_handler(obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
                
                # Convert to JSON string with proper datetime handling
                json_data = json.dumps(products_data, indent=2, default=datetime_handler, ensure_ascii=False)
                
                # Validate JSON before upload
                try:
                    json.loads(json_data)
                    print("JSON validation passed")
                except json.JSONDecodeError as e:
                    print(f"JSON validation failed: {e}")
                    return 1
                
                # Upload to ADLS
                upload_success = upload_to_adls(json_data=json_data, source_website="lifemobile.lk")
                
                if upload_success:
                    print("Data successfully uploaded to Azure Data Lake Storage!")
                    print(f"Final Summary:")
                    print(f"Products uploaded: {total_products:,}")
                    print(f"Variants uploaded: {total_variants:,}")
                    print(f"Data size: {len(json_data) / (1024*1024):.2f} MB")
                    print(f"Total time: {(datetime.now(timezone.utc) - start_time).total_seconds():.2f} seconds")
                else:
                    print("Failed to upload data to ADLS")
                    
            except Exception as e:
                print(f"Failed to upload to ADLS: {e}")
                logging.error(f"ADLS upload error: {e}", exc_info=True)
        else:
            print("\nAzure SDK not available - skipping upload")
            print("Install with: pip install azure-storage-blob")

        # # Final cleanup
        # try:
        #     cleanup_files = ["lifemobile_products.json", "lifemobile_products_fixed.json"]
        #     for cleanup_file in cleanup_files:
        #         if os.path.exists(cleanup_file):
        #             os.remove(cleanup_file)
        #             print(f"Final cleanup: Removed {cleanup_file}")
        # except Exception as e:
        #     print(f"Final cleanup warning: {e}")

        print(f"\nPipeline completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user.")
        return 1
    except Exception as e:
        print(f"\nPipeline failed: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user.")
        exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)