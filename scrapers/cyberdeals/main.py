#!/usr/bin/env python3
"""
CyberDeals Web Scraper - Enhanced Version
Scrapes product data from CyberDeals and saves as JSON
Additionally, uploads data to Azure Data Lake Storage (ADLS)

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
from azure.storage.blob import BlobServiceClient

try:
    from scrapers.cyberdeals.scripts.product_scraper_manager import run_scraper
except ImportError:
    # Fallback for direct execution
    from scripts.product_scraper_manager import run_scraper

def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    CyberDeals Web Scraper                    ║
║                  Enhanced Product Data Scraper               ║
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
    scrape_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    file_path = f"source_website={source_website}/scrape_date={scrape_date}/data.json"
    container_name = "raw-data"

    try:
        # --- 3. Connect to Azure and Upload ---
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)

        print(f"Uploading data to: {container_name}/{file_path}")
        
        # Upload the already prepared JSON string
        blob_client.upload_blob(json_data, overwrite=True)

        print("Upload to ADLS successful!")
        return True

    except Exception as e:
        print(f"ADLS upload error: {e}")
        logging.error(f"ADLS upload error: {e}", exc_info=True)
        return False

async def main():
    """Main entry point - scrape all products and save as JSON"""
    print_banner()
    print("Starting CyberDeals product scraping...")
    print(f"Scraping started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    try:
        # Apply nest_asyncio to make asyncio work in Jupyter
        nest_asyncio.apply()
        
        # Run the scraper and get total products scraped
        print("\n🚀 Scraping all products from CyberDeals...")
        total_products = await run_scraper()
        
        # Display results
        print(f"\n{'='*60}")
        print("✅ SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"📊 Products scraped: {total_products:,}")
        
        # Upload data to Azure Data Lake Storage
        try:
            print(f"\n{'='*60}")
            print("☁️ UPLOADING TO AZURE DATA LAKE STORAGE")
            print(f"{'='*60}")
            
            # Load the scraped data from the local file
            from config.scraper_config import OUTPUT_DIR, OUTPUT_FILE
            output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
            
            if os.path.exists(output_path):
                with open(output_path, 'r', encoding='utf-8') as f:
                    json_data = f.read()
                
                # Upload to ADLS
                upload_to_adls(json_data=json_data, source_website="cyberdeals")
                
                print(f"📤 Data uploaded to ADLS successfully")
                
                # Optionally remove local file to save space
                # os.remove(output_path)
                # print(f"🗑️ Local file removed to save space")
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
    """
    Main entry point for the CyberDeals scraper.
    Applies nest_asyncio to enable asyncio in Jupyter environments.
    """
    try:
        exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation interrupted by user.")
        exit(1)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    run()
