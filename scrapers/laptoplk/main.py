#!/usr/bin/env python3
"""
Laptop.lk Web Scraper
Scrapes product data from Laptop.lk and saves as JSON
Additionally, uploads data to Azure Data Lake Storage (ADLS)

Usage:
    python main.py
"""

import json
import asyncio
import httpx
import re
import time
import os
import platform
import logging
import base64 # Added for block blob upload
from selectolax.parser import HTMLParser 
from datetime import datetime, timezone
from typing import List, Dict, Optional, Set, Any, Tuple
from dotenv import load_dotenv

# Add Azure Storage imports
from azure.storage.blob import BlobServiceClient, ContentSettings # Added ContentSettings

# Import config and utils
import sys
# Add the current directory to the path to make relative imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.scraper_config import (
    SITEMAP_INDEX_URL, MAX_CONCURRENT_REQUESTS, MAX_RETRIES,
    DEFAULT_HEADERS, OUTPUT_DIR, OUTPUT_FILE, LOGGING_CONFIG,
    AZURE_CONTAINER_NAME, AZURE_SOURCE_WEBSITE
)
from utils.scraper_utils import (
    setup_logging, ensure_output_directory, save_json_data
)

# Find and load the root .env file
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
dotenv_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path)

# Setup logging
setup_logging(LOGGING_CONFIG)
logger = logging.getLogger(__name__)

def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║                       Laptop.lk Web Scraper                    ║
║                   Simple Product Data Scraper                  ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

# --- START: REPLACED UPLOAD LOGIC ---
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
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        logger.error("Azure connection string not found in environment variables.")
        return False

    # Define the blob path
    scrape_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    blob_path = f"source_website={source_website}/scrape_date={scrape_date}/{file_name}"
    container_name = AZURE_CONTAINER_NAME
    
    # Calculate data size
    data_bytes = json_data.encode('utf-8')
    data_size_mb = len(data_bytes) / (1024 * 1024)
    
    logger.info(f"Uploading {file_name} to: {container_name}/{blob_path}")
    logger.info(f"Data size: {data_size_mb:.2f} MB")
    
    try:
        # Connect to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Check if container exists
        try:
            container_client = blob_service_client.get_container_client(container_name)
            container_client.get_container_properties()
        except Exception:
            logger.info(f"Container '{container_name}' does not exist. Creating it...")
            blob_service_client.create_container(name=container_name)
            logger.info(f"Container '{container_name}' created successfully!")
        
        # Get the blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        # For small files (<1MB), use simple upload
        if data_size_mb < 1:
            logger.info(f"File size is small ({data_size_mb:.2f}MB), using simple upload")
            blob_client.upload_blob(
                data_bytes, 
                overwrite=True,
                content_settings=ContentSettings(content_type='application/json')
            )
            return True
        
        # For larger files, use block blobs with 1MB blocks
        logger.info(f"Using block blob upload with 1MB blocks")
        
        # Define block size (1MB)
        block_size = 1 * 1024 * 1024
        
        # Calculate number of blocks
        num_blocks = (len(data_bytes) + block_size - 1) // block_size  # Ceiling division
        
        # Generate block IDs
        block_ids = [base64.b64encode(f"block-{i}".encode()).decode() for i in range(num_blocks)]
        
        # Start time for tracking
        start_time = datetime.now()
        logger.info(f"Upload started at: {start_time.strftime('%H:%M:%S')}")
        logger.info(f"Uploading {num_blocks} blocks...")
        
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
                    logger.info(f"Uploading block {i+1}/{num_blocks} (Attempt {attempt+1}/{max_retries})...")
                    blob_client.stage_block(
                        block_id=block_ids[i],
                        data=block_data,
                        timeout=120  # 2-minute timeout per block
                    )
                    logger.info(f"✓ Block {i+1}/{num_blocks} uploaded")
                    break
                except Exception as e:
                    logger.error(f"✗ Error uploading block {i+1}: {str(e)}")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        logger.info(f"   Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to upload block {i+1} after {max_retries} attempts")
                        raise
        
        # Commit the blocks
        logger.info("All blocks uploaded. Committing block list...")
        blob_client.commit_block_list(
            block_ids,
            content_settings=ContentSettings(content_type='application/json')
        )
        
        # Calculate statistics
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Upload completed in {duration:.1f} seconds")
        if duration > 0:
            logger.info(f"Average upload speed: {data_size_mb / (duration/60):.2f}MB/minute")
        
        logger.info(f"Successfully uploaded {file_name} to ADLS!")
        return True
    
    except Exception as e:
        logger.error(f"ADLS upload error for {file_name}: {e}", exc_info=True)
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

# --- END: REPLACED UPLOAD LOGIC ---


class AsyncLaptopLKScraper:
    def __init__(self, max_connections: int = MAX_CONCURRENT_REQUESTS, max_retries: int = MAX_RETRIES):
        self.source_website = AZURE_SOURCE_WEBSITE
        self.scrape_timestamp = datetime.now(timezone.utc).isoformat()
        self.shop_phone = "+94 77 733 6464"
        self.shop_whatsapp = "+94 77 733 6464"
        self.max_retries = max_retries
        self.headers = DEFAULT_HEADERS
        self.semaphore = asyncio.Semaphore(max_connections)
        self.success_count = 0
        self.error_count = 0

    async def fetch_page(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    response = await client.get(url, headers=self.headers, timeout=30, follow_redirects=True)
                    response.raise_for_status()
                    self.success_count += 1
                    return response.text
                except (httpx.RequestError, httpx.HTTPStatusError) as e:
                    if attempt + 1 == self.max_retries: break
                    await asyncio.sleep(2 ** attempt)
                except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, 
                        httpx.RemoteProtocolError) as e:
                    logger.warning(f"Connection error on attempt {attempt+1} for {url}: {str(e)}")
                    if attempt + 1 == self.max_retries: break
                    await asyncio.sleep(3 ** attempt)  # Longer backoff for connection issues
                except Exception as e:
                    # Handle TLS/SSL errors specifically
                    if "SSL" in str(e) or "TLS" in str(e) or "EndOfStream" in str(e):
                        logger.warning(f"TLS/SSL error on {url}: {str(e)}. Trying alternative approach...")
                        
                        # Try curl as a fallback with relaxed SSL settings
                        try:
                            import subprocess
                            
                            cmd = [
                                "curl", "-s", "-L", 
                                "-H", f"User-Agent: {self.headers['User-Agent']}", 
                                "--insecure",  # Allows "insecure" SSL connections
                                url
                            ]
                            
                            # Execute curl command
                            logger.info(f"Trying curl fallback for {url}")
                            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                            
                            if result.returncode == 0 and result.stdout:
                                logger.info(f"Successfully fetched {url} using curl fallback")
                                self.success_count += 1
                                return result.stdout
                        except Exception as curl_err:
                            logger.warning(f"Curl fallback failed for {url}: {curl_err}")
                    
                    # For other unexpected errors
                    if attempt + 1 == self.max_retries: break
                    await asyncio.sleep(2 ** attempt)
            
            self.error_count += 1
        return None

    def parse_product_data(self, html: str, url: str) -> Optional[Dict]:
        try:
            tree = HTMLParser(html)
            product_container = tree.css_first("div[id^=product-]")
            if not product_container: 
                self.error_count += 1
                return None
            
            title_node = product_container.css_first("h1.product_title")
            title = title_node.text(strip=True) if title_node else None

            product_id = product_container.id.split('-')[-1] if product_container.id else None
            
            desc_node = product_container.css_first("div#tab-description, div.woocommerce-tabs")
            description_html = desc_node.html if desc_node else None

            category_nodes = product_container.css("span.posted_in a")
            all_categories = [node.text(strip=True) for node in category_nodes]
            
            if not all_categories:
                logger.debug("No categories found with standard selector, trying alternative...")
                alt_category_nodes = tree.css("a[rel='tag']")
                if alt_category_nodes:
                    all_categories = [node.text(strip=True) for node in alt_category_nodes]
                    logger.debug(f"Found {len(all_categories)} categories with alternative selector")
            
            brand = next((cat for cat in all_categories if cat.lower() in ['hp', 'dell', 'apple', 'lenovo', 'asus', 'msi', 'acer', 'samsung']), None)
            category_path = [c for c in all_categories if c.lower() != (brand or '').lower()]

            image_nodes = product_container.css("div.woocommerce-product-gallery__image a")
            image_urls = [node.attributes.get('href') for node in image_nodes]

            payment_elements = tree.css("div.yith-wapo-block[data-block-name*='Payment Methods'] .yith-wapo-option-price .woocommerce-Price-amount")
            if payment_elements:
                prices = []
                for el in payment_elements:
                    price_text = el.text(strip=True)
                    clean_price = re.sub(r'[^0-9.]', '', price_text)
                    if clean_price:
                        prices.append(clean_price)
                
                if prices:
                    price_current = min(prices)
                else:
                    price_curr_node = product_container.css_first("div.product-actions .price .woocommerce-Price-amount")
                    if price_curr_node:
                        price_text = price_curr_node.text(strip=True)
                        price_current = re.sub(r'[^0-9.]', '', price_text)
                        price_current = "null" if not price_current else price_current
                    else:
                        price_current = "null"
            else:
                price_curr_node = product_container.css_first("div.product-actions .price .woocommerce-Price-amount")
                if not price_curr_node:
                    price_curr_node = product_container.css_first("p.price ins .amount, span.electro-price ins .amount, p.price > .amount, span.electro-price > .amount, .summary .price .woocommerce-Price-amount")
                
                if price_curr_node:
                    price_text = price_curr_node.text(strip=True)
                    price_current = re.sub(r'[^0-9.]', '', price_text)
                    price_current = "null" if not price_current else price_current
                else:
                    price_current = "null"
                    logger.debug("No price element found for product, setting current price to null")
            
            has_discount = product_container.css_first("div.product-actions .price ins") is not None
            
            price_original = None
            if has_discount:
                price_orig_node = product_container.css_first("div.product-actions .price del .woocommerce-Price-amount")
                if not price_orig_node:
                    price_orig_node = product_container.css_first("p.price del .woocommerce-Price-amount, span.electro-price del .woocommerce-Price-amount")
                
                if price_orig_node:
                    price_text = price_orig_node.text(strip=True)
                    price_original = re.sub(r'[^0-9.]', '', price_text)
                    logger.debug(f"Found original price: {price_text} -> {price_original}")
            
            if price_original is None:
                logger.debug("No original price found or no discount active")

            availability_text = "Out of Stock" if product_container.css_first("p.stock.out-of-stock") else "In Stock"

            warranty_text = None
            warranty_img = product_container.css_first("img[alt*='warranty' i]")
            if warranty_img and 'alt' in warranty_img.attributes:
                warranty_text = warranty_img.attributes['alt'].replace('Year-warranty', ' Year Warranty').replace('-', ' ')

            final_original_price = "null" if price_original is None else price_original
            
            variants = [{"variant_id_native": product_id, "variant_title": "Default", "price_current": price_current, "price_original": final_original_price, "currency": "LKR", "availability_text": availability_text}]
            
            self.success_count += 1
            return {"product_id_native": product_id, "product_url": url, "product_title": title, "warranty": warranty_text, "description_html": description_html, "brand": brand, "category_path": category_path, "image_urls": image_urls, "variants": variants, "metadata": {"source_website": self.source_website, "shop_contact_phone": self.shop_phone,"shop_contact_whatsapp": self.shop_whatsapp, "scrape_timestamp": self.scrape_timestamp}}
        except Exception as e:
            logger.error(f"Error parsing product data: {e}", exc_info=True)
            self.error_count += 1
            return None

    def save_data(self, data: List[Dict[str, Any]], filename: str):
        output_path = os.path.join(OUTPUT_DIR, filename)
        ensure_output_directory(OUTPUT_DIR)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Data successfully saved to {output_path}")

async def fetch_and_parse_product(client: httpx.AsyncClient, scraper: AsyncLaptopLKScraper, url: str) -> Optional[Dict]:
    try:
        html = await scraper.fetch_page(client, url)
        if html:
            return scraper.parse_product_data(html, url)
        return None
    except RuntimeError as e:
        # Handle "client has been closed" errors gracefully
        if "client has been closed" in str(e):
            logger.warning(f"Client closed while processing {url}. This is expected during shutdown.")
        else:
            logger.warning(f"Runtime error while processing {url}: {str(e)}")
        return None
    except Exception as e:
        logger.warning(f"Error processing {url}: {str(e)}")
        return None

async def process_in_batches(tasks, batch_size=50):
    results = []
    total = len(tasks)
    processed = 0
    failed = 0
    start_time = time.time()
    
    for i in range(0, total, batch_size):
        batch = tasks[i:i+batch_size]
        try:
            # Use return_exceptions=True to prevent one failure from stopping everything
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            # Process results, filter out exceptions
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.warning(f"Task error: {str(result)}")
                    failed += 1
                    results.append(None)  # Add None for failed tasks
                else:
                    results.append(result)
        except Exception as e:
            # This shouldn't happen with return_exceptions=True, but just in case
            logger.error(f"Batch processing error: {str(e)}")
            # Add None results for the whole batch
            results.extend([None] * len(batch))
            failed += len(batch)
        
        processed += len(batch)
        elapsed = time.time() - start_time
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        
        print(f"\rProcessed: {processed}/{total} ({processed/total*100:.1f}%) | "
              f"Failed: {failed} | Rate: {rate:.1f} items/sec | ETA: {eta:.1f}s", end="")
        
        # Add a small delay between batches to prevent overwhelming the server
        await asyncio.sleep(0.5)
    
    print()
    return results

async def run_scraper() -> Tuple[int, Dict]:
    start_time = time.time()
    stats = {
        "categories_found": 0,
        "product_urls_found": 0,
        "products_scraped": 0,
        "errors": 0,
        "success_rate": 0,
        "total_time": 0,
        "products_per_second": 0
    }
    
    scraper = AsyncLaptopLKScraper()
    
    logger.info(f"Starting scrape of laptop.lk")
    fetch_start = time.time()
    
    async with httpx.AsyncClient() as client:
        logger.info(f"Fetching sitemap index: {SITEMAP_INDEX_URL}")
        index_xml = await scraper.fetch_page(client, SITEMAP_INDEX_URL)
        if not index_xml: 
            logger.error("Failed to fetch sitemap index")
            return 0, stats

        product_sitemap_urls = [node.text() for node in HTMLParser(index_xml).css('loc') if 'product-sitemap' in node.text()]
        logger.info(f"Found {len(product_sitemap_urls)} product sitemaps")
        
        logger.info("Fetching product URLs from sitemaps")
        sitemap_tasks = [scraper.fetch_page(client, url) for url in product_sitemap_urls]
        sitemap_xmls = await asyncio.gather(*sitemap_tasks)
        
        unique_product_urls = {loc.text() for xml in sitemap_xmls if xml for loc in HTMLParser(xml).css('url > loc')}
        product_urls_list = list(unique_product_urls)
        
        stats["product_urls_found"] = len(product_urls_list)
        logger.info(f"Found {len(product_urls_list)} unique product URLs to scrape.")
        if not product_urls_list: return 0, stats

        fetch_end = time.time()
        logger.info(f"URL discovery took {fetch_end - fetch_start:.2f} seconds")

        logger.info(f"Scraping {len(product_urls_list)} products")
        
        # Use a smaller batch size for more stability in Docker environment
        # This helps prevent TLS handshake timeouts and client closure issues
        small_batch_size = 25  # Reduced from 50 to 25
        
        try:
            # Create tasks with proper exception handling
            tasks = [fetch_and_parse_product(client, scraper, url) for url in product_urls_list]
            results = await process_in_batches(tasks, batch_size=small_batch_size)
            all_products_data = [item for item in results if item is not None]
        except Exception as e:
            logger.error(f"Error during batch processing: {str(e)}")
            # Try to recover whatever results we have
            all_products_data = [item for item in results if item is not None] if 'results' in locals() else []
            logger.info(f"Recovered {len(all_products_data)} products despite error")
        
        # Make sure we have stats even if there was an error
        stats["products_scraped"] = len(all_products_data)
        stats["errors"] = scraper.error_count
        stats["success_rate"] = (len(all_products_data) / len(product_urls_list)) * 100 if product_urls_list else 0
    
    # Save data locally
    scraper.save_data(all_products_data, OUTPUT_FILE)
    
    # --- MODIFIED: Simplified upload call ---
    try:
        logger.info("Uploading data to Azure Data Lake Storage")
        json_data = json.dumps(all_products_data, indent=2, ensure_ascii=False)
        
        # The new upload function handles large files automatically
        upload_success = upload_to_adls(
            json_data=json_data, 
            source_website=AZURE_SOURCE_WEBSITE
        )
        
        if upload_success:
            logger.info("Data successfully uploaded to Azure Data Lake Storage")
        else:
            logger.error("Failed to upload data to Azure Data Lake Storage")
            
    except Exception as e:
        logger.error(f"Error during the upload process to Azure: {e}", exc_info=True)
    
    # Calculate final stats
    end_time = time.time()
    stats["total_time"] = end_time - start_time
    stats["products_per_second"] = len(all_products_data) / stats["total_time"] if stats["total_time"] > 0 else 0
    
    return len(all_products_data), stats

async def main():
    """Main entry point - scrape all products and upload to ADLS"""
    print_banner()
    print(f"Scraping started at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    try:
        ensure_output_directory(OUTPUT_DIR)
        product_count, stats = await run_scraper()
        
        print("\n" + "="*60)
        print("🚀 SCRAPE PERFORMANCE SUMMARY 🚀")
        print("="*60)
        print(f"📊 RESULTS:")
        print(f"   • Products Found:     {stats['product_urls_found']}")
        print(f"   • Products Scraped:   {stats['products_scraped']} ({stats['success_rate']:.1f}% success)")
        print(f"   • Failed Products:    {stats['errors']}")
        
        print(f"\n⏱️ TIMING:")
        print(f"   • Total Time:         {stats['total_time']:.2f} seconds ({stats['total_time']/60:.2f} minutes)")
        print(f"   • Processing Speed:   {stats['products_per_second']:.2f} products/second")
        if product_count > 0:
            print(f"   • Average Time/Item:  {stats['total_time']/product_count*1000:.2f} ms per product")
        
        print(f"\n💻 SYSTEM INFO:")
        print(f"   • OS:                 {platform.system()} {platform.release()}")
        print(f"   • Python:             {platform.python_version()}")
        print(f"   • CPU:                {platform.processor()}")
        print(f"   • CPU Cores:          {os.cpu_count() or 'Unknown'}")
        
        print("="*60)
        
        return 0
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
        return 1
    except Exception as e:
        print(f"\nScraping failed: {e}")
        logger.error(f"Fatal error: {e}", exc_info=True)
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
