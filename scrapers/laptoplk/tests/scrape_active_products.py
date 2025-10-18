#!/usr/bin/env python3
"""
Find and scrape 10 active products from laptop.lk with full details
"""
import sys
import os
import asyncio
import httpx
import json
import time
import re
from selectolax.parser import HTMLParser
from datetime import datetime, timezone

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class and config
from main import AsyncLaptopLKScraper
from config.scraper_config import DEFAULT_HEADERS

async def discover_product_urls(client, base_url="https://www.laptop.lk/index.php/shop/", num_products=10):
    """Find active product URLs by crawling the shop page"""
    print(f"Discovering active product URLs from {base_url}...")
    
    # Start with the shop page
    try:
        response = await client.get(base_url, headers=DEFAULT_HEADERS, follow_redirects=True)
        response.raise_for_status()
        
        # Parse the HTML
        tree = HTMLParser(response.text)
        
        # Find all product links - typical WooCommerce structure
        product_links = tree.css("li.product a.woocommerce-LoopProduct-link")
        discovered_urls = []
        
        for link in product_links:
            if 'href' in link.attributes:
                product_url = link.attributes['href']
                if "/product/" in product_url:
                    discovered_urls.append(product_url)
                    if len(discovered_urls) >= num_products:
                        break
        
        # If we don't have enough products, check the next page
        if len(discovered_urls) < num_products:
            next_page_link = tree.css_first("a.next.page-numbers")
            if next_page_link and 'href' in next_page_link.attributes:
                next_page_url = next_page_link.attributes['href']
                more_urls = await discover_product_urls(client, next_page_url, num_products - len(discovered_urls))
                discovered_urls.extend(more_urls)
        
        # Return the URLs we found (limited to num_products)
        return discovered_urls[:num_products]
        
    except Exception as e:
        print(f"Error discovering product URLs: {str(e)}")
        return []

async def scrape_active_products(num_products=10):
    """Find active products and scrape their full details"""
    print(f"\n{'='*80}")
    print(f" 🛒 LAPTOP.LK ACTIVE PRODUCTS SCRAPER")
    print(f"{'='*80}")
    
    start_time = time.time()
    
    # Initialize scraper
    scraper = AsyncLaptopLKScraper()
    results = []
    failed = []
    
    # Create a client with increased timeout and retry logic
    transport = httpx.AsyncHTTPTransport(retries=3)
    limits = httpx.Limits(max_connections=5)
    
    async with httpx.AsyncClient(transport=transport, limits=limits, timeout=30, follow_redirects=True) as client:
        # First, discover active product URLs
        product_urls = await discover_product_urls(client, num_products=num_products)
        
        if not product_urls:
            print("Failed to discover any active product URLs. Exiting.")
            return []
        
        print(f"\nFound {len(product_urls)} active products. Attempting to scrape them...")
        
        # Create tasks for all products
        tasks = []
        for url in product_urls:
            task = asyncio.create_task(fetch_product_data(client, scraper, url))
            tasks.append(task)
        
        # Wait for all tasks to complete
        products = await asyncio.gather(*tasks)
        
        # Process results
        for i, (url, product_data) in enumerate(zip(product_urls, products)):
            if product_data:
                results.append(product_data)
                print(f"✅ [{i+1}/{len(product_urls)}] Successfully scraped: {product_data['product_title']}")
            else:
                failed.append(url)
                print(f"❌ [{i+1}/{len(product_urls)}] Failed to scrape: {url}")
    
    # Save the results to a JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.path.dirname(__file__), f"active_products_{timestamp}.json")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Print summary
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f"\n{'='*80}")
    print(f"SCRAPE SUMMARY")
    print(f"{'='*80}")
    print(f"Total products attempted: {len(product_urls)}")
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
    asyncio.run(scrape_active_products(10))