#!/usr/bin/env python3
"""
Scrape 10 specific products from laptop.lk with full details
"""
import sys
import os
import asyncio
import httpx
import json
import time
from datetime import datetime, timezone

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper

# List of specific product URLs to scrape
PRODUCT_URLS = [
    # These are verified working product URLs from laptop.lk
    "https://www.laptop.lk/index.php/product/logitech-shifter/",
    "https://www.laptop.lk/index.php/product/logitech-mx-master-3s-wireless-mouse/",
    "https://www.laptop.lk/index.php/product/logitech-pebble-mouse-2-m350s/",
    "https://www.laptop.lk/index.php/product/logitech-bluetooth-multi-device-keyboard-k380/",
    "https://www.laptop.lk/index.php/product/kodak-printomatic-digital-instant-print-camera-blue/",
    "https://www.laptop.lk/index.php/product/xiaomi-smart-band-8-active/",
    "https://www.laptop.lk/index.php/product/canon-eos-r50-mirrorless-camera-with-18-45mm-lens/",
    "https://www.laptop.lk/index.php/product/epson-ecotank-l3250/",
    "https://www.laptop.lk/index.php/product/samsung-galaxy-book4-core-ultra-7-16gb-1tb-16-3k/",
    "https://www.laptop.lk/index.php/product/dell-latitude-7490-i7-8650u-16gb-512gb/",
]

async def scrape_specific_products():
    """Scrape the specific list of products"""
    print(f"\n{'='*80}")
    print(f" 🛒 LAPTOP.LK SPECIFIC PRODUCTS SCRAPER")
    print(f"{'='*80}")
    
    start_time = time.time()
    
    # Initialize scraper
    scraper = AsyncLaptopLKScraper()
    results = []
    failed = []
    
    # Create a client with increased timeout and retry logic
    transport = httpx.AsyncHTTPTransport(retries=3)
    limits = httpx.Limits(max_connections=5)
    
    print(f"\nAttempting to scrape {len(PRODUCT_URLS)} products...")
    
    async with httpx.AsyncClient(transport=transport, limits=limits, timeout=30, follow_redirects=True) as client:
        # Create tasks for all products
        tasks = []
        for url in PRODUCT_URLS:
            task = asyncio.create_task(fetch_product_data(client, scraper, url))
            tasks.append(task)
        
        # Wait for all tasks to complete
        products = await asyncio.gather(*tasks)
        
        # Process results
        for i, (url, product_data) in enumerate(zip(PRODUCT_URLS, products)):
            if product_data:
                results.append(product_data)
                print(f"✅ [{i+1}/{len(PRODUCT_URLS)}] Successfully scraped: {product_data['product_title']}")
            else:
                failed.append(url)
                print(f"❌ [{i+1}/{len(PRODUCT_URLS)}] Failed to scrape: {url}")
    
    # Save the results to a JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.path.dirname(__file__), f"sample_products_{timestamp}.json")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Print summary
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\n{'='*80}")
    print(f"SCRAPE SUMMARY")
    print(f"{'='*80}")
    print(f"Total products attempted: {len(PRODUCT_URLS)}")
    print(f"Successfully scraped:     {len(results)}")
    print(f"Failed to scrape:         {len(failed)}")
    print(f"Time taken:               {elapsed:.2f} seconds")
    print(f"Results saved to:         {output_path}")
    print(f"{'='*80}\n")
    
    return results

async def fetch_product_data(client, scraper, url):
    """Fetch and parse a single product"""
    try:
        html = await scraper.fetch_page(client, url)
        if not html:
            print(f"Failed to fetch HTML for {url}")
            return None
            
        product_data = scraper.parse_product_data(html, url)
        return product_data
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return None

if __name__ == "__main__":
    asyncio.run(scrape_specific_products())