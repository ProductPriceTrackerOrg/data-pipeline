#!/usr/bin/env python3
"""
SimplyTek Web Scraper - Simplified Version
Scrapes product data from SimplyTek and saves as JSON
Additionally, uploads data to Azure Data Lake Storage (ADLS)

Usage:
    python main.py
"""

import asyncio
import logging
import sys
import os
import json
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the current directory to the path to allow imports
sys.path.append('.')

# Azure Storage imports
from azure.storage.blob import BlobServiceClient

from scripts.product_scraper_manager import ScrapingManager, setup_scraping_environment


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    SimplyTek Web Scraper                     ║
║                  Simple Product Data Scraper                 ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def upload_to_adls(json_data: str, source_website: str):
    """
    Uploads a JSON string to ADLS as a single JSON file.
    
    Args:
        json_data: Ready-to-upload JSON string with properly serialized data
        source_website: Name of the source website (used for partitioning)
    """
    # --- 1. Get Azure Connection String from Environment Variable ---
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("Azure connection string not found in environment variables.")

    # --- 2. Define the partitioned path ---
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    file_path = f"source_website={source_website}/scrape_date={scrape_date}/data.json"
    container_name = "raw-data"

    try:
        # --- 3. Configure Azure Storage Client with increased timeouts and retry policy ---
        from azure.storage.blob import BlobServiceClient, ContentSettings
        from azure.core.pipeline.policies import RetryPolicy
        
        # Configure service client with timeouts (in seconds)
        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string,
            connection_timeout=60,  # Connection timeout
            read_timeout=300,       # Read timeout
            socket_timeout=300      # Socket timeout
        )
        
        # Get blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)

        print(f"Uploading data to: {container_name}/{file_path}")
        
        # For large files, use chunked upload with increased timeout
        # Upload with extended timeout and JSON content type
        blob_client.upload_blob(
            json_data, 
            overwrite=True,
            content_settings=ContentSettings(content_type='application/json'),
            timeout=300
        )

        print("Upload to ADLS successful!")
        return True

    except Exception as e:
        print(f"ADLS upload error: {e}")
        logging.error(f"ADLS upload error: {e}", exc_info=True)
        return False


async def main():
    """Main entry point - scrape all products and save as JSON"""
    print_banner()
    print("Starting SimplyTek product scraping...")
    print(f"Scraping started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Setup environment (logging, directories, etc.)
        setup_scraping_environment()
        
        # Create scraping manager with save_locally=False to skip local file saving
        manager = ScrapingManager(save_locally=False)
        
        # Run full scraping
        print("\n Scraping all products from SimplyTek...")
        result = await manager.run_full_scraping()
        
        # Display results
        print(f"\n{'='*60}")
        print(" SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f" Products scraped: {result.total_products:,}")
        print(f" Variants scraped: {result.total_variants:,}")
        print(f" Categories processed: {len(result.categories_scraped)}")
        print(f" Duration: {result.scraping_metadata.get('scraping_duration', 'N/A')}")
        
        # Upload data to Azure Data Lake Storage
        try:
            print(f"\n{'='*60}")
            print(" UPLOADING TO AZURE DATA LAKE STORAGE")
            print(f"{'='*60}")
            
            # Use Pydantic's built-in JSON serialization to handle datetime objects properly
            # Support both Pydantic v1 and v2 APIs
            try:
                # Try Pydantic v2 API first
                json_data = json.dumps([product.model_dump(mode='json') for product in result.products], indent=2)
            except AttributeError:
                print("Using Pydantic v1 API (dict method) for serialization")
                # Fall back to Pydantic v1 API if model_dump is not available
                try:
                    json_data = json.dumps([product.dict() for product in result.products], indent=2)
                except Exception as e:
                    print(f"Failed to serialize with dict(): {e}")
                    # Try a custom serialization approach as last resort
                    def datetime_handler(x):
                        if isinstance(x, datetime):
                            return x.isoformat()
                        raise TypeError(f"Object of type {type(x)} is not JSON serializable")
                    
                    json_data = json.dumps([product.dict() for product in result.products], 
                                          default=datetime_handler, indent=2)
            
            # Upload to ADLS
            upload_to_adls(json_data=json_data, source_website="simplytek")
            
            print(f" Data uploaded to ADLS")
        except Exception as e:
            print(f"Failed to upload to ADLS: {e}")
            logging.error(f"ADLS upload error: {e}", exc_info=True)

        return 0
        
    except KeyboardInterrupt:
        print("\n  Scraping interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n Scraping failed: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user.")
        exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)