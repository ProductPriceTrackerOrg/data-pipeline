"""
Manager for orchestrating the CyberDeals scraping process
"""
import asyncio
import httpx
import time
import os
from typing import List, Dict, Any
from selectolax.parser import HTMLParser

try:
    from ..config.scraper_config import (
        SITEMAP_URL, 
        BATCH_SIZE, 
        OUTPUT_DIR, 
        OUTPUT_FILE,
        MAX_CONNECTIONS
    )
    from ..scripts.product_scraper_core import AsyncCyberDealsScraper
    from ..utils.scraper_utils import ensure_output_dir
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.scraper_config import (
        SITEMAP_URL, 
        BATCH_SIZE, 
        OUTPUT_DIR, 
        OUTPUT_FILE,
        MAX_CONNECTIONS
    )
    from scripts.product_scraper_core import AsyncCyberDealsScraper
    from utils.scraper_utils import ensure_output_dir

async def batch_process_urls(
    client: httpx.AsyncClient, 
    scraper: AsyncCyberDealsScraper, 
    urls: List[str], 
    batch_size: int = BATCH_SIZE
) -> List[Dict[str, Any]]:
    """Process URLs in batches to avoid memory issues"""
    all_products = []
    total = len(urls)
    for i in range(0, total, batch_size):
        batch_urls = urls[i:i + batch_size]
        print(f"\n📦 Processing batch {i // batch_size + 1} ({len(batch_urls)} products)...")
        tasks = [scraper.fetch_page(client, url) for url in batch_urls]
        
        # Use asyncio.gather instead of tqdm.gather for production
        print(f"⚡ Downloading batch {i // batch_size + 1}...")
        html_pages = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and process successful responses
        successful_pages = [
            (html, url) for html, url in zip(html_pages, batch_urls) 
            if html and not isinstance(html, Exception)
        ]
        
        print(f"✅ Successfully downloaded {len(successful_pages)}/{len(batch_urls)} pages")
        
        parsed_products = [
            scraper.parse_product_page(html, url)
            for html, url in successful_pages
        ]
        batch_products = [p for p in parsed_products if p]
        all_products.extend(batch_products)
        
        print(f"📊 Parsed {len(batch_products)} products from batch {i // batch_size + 1}")
    
    print(f"\n🎯 Total products successfully scraped: {len(all_products)}")
    return all_products

async def run_scraper() -> int:
    """Main function to run the scraper"""
    start_time = time.time()
    scraper = AsyncCyberDealsScraper(max_connections=MAX_CONNECTIONS)

    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        print(f"🔍 Fetching sitemap index: {SITEMAP_URL}")
        index_xml = await scraper.fetch_page(client, SITEMAP_URL)
        if not index_xml:
            print("❌ Failed to load sitemap index.")
            return 0

        # Extract product sitemap URLs
        sitemap_urls = [
            node.text() for node in HTMLParser(index_xml).css("loc")
            if "product-sitemap" in node.text()
        ]

        # Fetch all product sitemaps
        sitemap_pages = await asyncio.gather(*[scraper.fetch_page(client, url) for url in sitemap_urls])

        # Extract all product URLs
        product_urls = [
            node.text() for xml in sitemap_pages if xml
            for node in HTMLParser(xml).css("url > loc")
        ]

        print(f"🔗 Found {len(product_urls)} product URLs.")

        # Process products in batches
        products = await batch_process_urls(client, scraper, product_urls)

        # Ensure output directory exists
        ensure_output_dir(OUTPUT_DIR)
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        scraper.save_data(products, output_path)

        # Print summary
        end_time = time.time()
        print("\n" + "=" * 50)
        print("⚡ SCRAPE SUMMARY ⚡")
        print("=" * 50)
        print(f"✅ Total Products Scraped: {len(products)}")
        print(f"⏱️ Total Time Taken: {end_time - start_time:.2f} seconds")
        print("=" * 50)
        return len(products)
