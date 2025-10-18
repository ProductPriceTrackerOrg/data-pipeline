#!/usr/bin/env python3
"""
CyberDeals Web Scraper - Enhanced Version
Scrapes product data from CyberDeals and saves as JSON
Additionally, uploads data to Azure Data Lake Storage (ADLS) as a single file.

Usage:
    python main.py
"""

import nest_asyncio
import asyncio
import logging
import sys
import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from the global .env file (go up 3 levels to data-pipeline root)
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
load_dotenv(env_path)

# Add the current directory to Python path for direct execution
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Azure Storage imports
from azure.storage.blob import BlobServiceClient, ContentSettings

try:
    from scrapers.cyberdeals.scripts.product_scraper_manager import run_scraper
except ImportError:
    # Fallback for direct execution
    from scripts.product_scraper_manager import run_scraper

def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║                   CyberDeals Web Scraper                     ║
║               Enhanced Product Data Scraper                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def upload_to_adls_block_blob(json_data: str, source_website: str, file_name: str = "data.json"):
    """
    Uploads a JSON string to ADLS as a single JSON file using block blob upload.
    This method is more resilient to network issues and timeouts by breaking the
    file into smaller blocks and uploading each block separately.
    
    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
        file_name: Name of the file to upload (default: data.json)
    """
    import base64
    import tempfile
    import time
    
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("Azure connection string not found in environment variables.")

    # Define the blob path
    scrape_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    blob_path = f"source_website={source_website}/scrape_date={scrape_date}/{file_name}"
    container_name = "raw-data"
    
    # Calculate data size
    data_bytes = json_data.encode('utf-8')
    data_size_mb = len(data_bytes) / (1024 * 1024)
    
    print(f"Uploading {file_name} to: {container_name}/{blob_path}")
    print(f"Data size: {data_size_mb:.2f} MB")
    
    try:
        # Connect to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Check if container exists
        container_exists = any(c.name == container_name for c in blob_service_client.list_containers())
                
        if not container_exists:
            print(f"Container '{container_name}' does not exist. Creating it...")
            blob_service_client.create_container(name=container_name)
            print(f"Container '{container_name}' created successfully!")
        
        # Get the blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # For small files (<1MB), use simple upload
        if data_size_mb < 1:
            print(f"File size is small ({data_size_mb:.2f}MB), using simple upload")
            blob_client.upload_blob(
                data_bytes, 
                overwrite=True,
                content_settings=ContentSettings(content_type='application/json')
            )
            return True
        
        # For larger files, use block blobs with 1MB blocks
        print(f"Using block blob upload with 1MB blocks")
        
        # Define block size (1MB)
        block_size = 1 * 1024 * 1024
        
        # Calculate number of blocks
        num_blocks = (len(data_bytes) + block_size - 1) // block_size  # Ceiling division
        
        # Generate block IDs
        block_ids = [base64.b64encode(f"block-{i}".encode()).decode() for i in range(num_blocks)]
        
        # Start time for tracking
        start_time = datetime.now()
        print(f"Upload started at: {start_time.strftime('%H:%M:%S')}")
        print(f"Uploading {num_blocks} blocks...")
        
        # Upload each block with retries
        for i in range(num_blocks):
            # Get the block data
            start_pos = i * block_size
            end_pos = min(start_pos + block_size, len(data_bytes))
            block_data = data_bytes[start_pos:end_pos]
            
            # Upload the block with retries
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    print(f"Uploading block {i+1}/{num_blocks} (Attempt {attempt+1}/{max_retries})...")
                    blob_client.stage_block(
                        block_id=block_ids[i],
                        data=block_data,
                        timeout=120  # 2-minute timeout per block
                    )
                    print(f"✓ Block {i+1}/{num_blocks} uploaded")
                    break
                except Exception as e:
                    print(f"✗ Error uploading block {i+1}: {str(e)}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"  Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        print(f"Failed to upload block {i+1} after {max_retries} attempts")
                        raise
        
        # Commit the blocks
        print("All blocks uploaded. Committing block list...")
        blob_client.commit_block_list(
            block_ids,
            content_settings=ContentSettings(content_type='application/json')
        )
        
        # Calculate statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Upload completed in {duration:.1f} seconds")
        print(f"Average upload speed: {data_size_mb / (duration/60):.2f}MB/minute")
        
        print(f"Successfully uploaded {file_name} to ADLS!")
        return True
    
    except Exception as e:
        print(f"ADLS upload error for {file_name}: {e}")
        logging.error(f"ADLS upload error for {file_name}: {e}", exc_info=True)
        return False

def upload_to_adls(json_data: str, source_website: str, file_name: str = "data.json"):
    """
    Uploads a JSON string to ADLS as a single JSON file.
    This is a wrapper function that uses the block blob upload method,
    which is more resilient to network issues.
    
    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
        file_name: Name of the file to upload (default: data.json)
    """
    # Use the block blob upload method which is more resilient
    return upload_to_adls_block_blob(json_data, source_website, file_name)

async def main():
    """Main entry point - scrape all products and save as JSON"""
    print_banner()
    print("Starting CyberDeals product scraping...")
    print(f"Scraping started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    try:
        nest_asyncio.apply()
        
        print("\n🚀 Scraping all products from CyberDeals...")
        total_products = await run_scraper()
        
        print(f"\n{'='*60}")
        print("✅ SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"📊 Products scraped: {total_products:,}")
        
        try:
            print(f"\n{'='*60}")
            print("☁️ UPLOADING TO AZURE DATA LAKE STORAGE")
            print(f"{'='*60}")
            
            from config.scraper_config import OUTPUT_DIR, OUTPUT_FILE
            output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
            
            if os.path.exists(output_path):
                with open(output_path, 'r', encoding='utf-8') as f:
                    raw_json_data = f.read()

                data_size_mb = len(raw_json_data.encode('utf-8')) / (1024 * 1024)
                print(f"Total data size: {data_size_mb:.2f} MB")
                
                print(f"Uploading data to ADLS as a single file (data.json)")
                upload_success = upload_to_adls(
                    json_data=raw_json_data,
                    source_website="cyberdeals",
                    file_name="data.json" # Always use the final filename
                )
                
                if upload_success:
                    print(f"📤 Data uploaded to ADLS successfully")
                else:
                    print(f"❌ Failed to upload data to ADLS")
            else:
                print(f"❌ No scraped data file found at {output_path}")
            
        except Exception as e:
            print(f"❌ Failed to upload to ADLS: {e}")
            logging.error(f"ADLS upload error: {e}", exc_info=True)

        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Scraping interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n❌ Scraping failed: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1

def run():
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run()


