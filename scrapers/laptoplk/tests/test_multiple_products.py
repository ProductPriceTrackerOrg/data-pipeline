#!/usr/bin/env python3
"""
Test script to scrape 5 products from laptop.lk to confirm price extraction logic
"""
import sys
import os
import asyncio
import httpx
import json
import re

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper

# Use verified working product URLs from laptop.lk
SAMPLE_PRODUCTS = [
    "https://www.laptop.lk/index.php/product/logitech-shifter/",  # Known to have original price
    "https://www.laptop.lk/index.php/product/logitech-mx-master-3s-wireless-mouse/",  # Verified working URL
    "https://www.laptop.lk/index.php/product/tp-link-deco-m5-1-pack/",  # Try another product
    "https://www.laptop.lk/index.php/product/dell-inspiron-5425-ryzen-7-5825u-8gb-512gb/",  # Try another product
    "https://www.laptop.lk/index.php/product/apple-watch-series-8-gps-45mm/",  # Try another product
]

async def fetch_product_data(url):
    """Fetch and parse a single product"""
    scraper = AsyncLaptopLKScraper()
    
    async with httpx.AsyncClient() as client:
        try:
            print(f"Fetching: {url}")
            html = await scraper.fetch_page(client, url)
            if not html:
                print(f"Failed to fetch {url}")
                return None
            
            product_data = scraper.parse_product_data(html, url)
            if not product_data:
                print(f"Failed to parse {url}")
                return None
                
            return product_data
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
            return None

async def test_multiple_products():
    """Test price extraction on multiple products"""
    results = []
    tasks = []
    
    # Create tasks for all products
    for url in SAMPLE_PRODUCTS:
        task = asyncio.create_task(fetch_product_data(url))
        tasks.append(task)
    
    # Wait for all tasks to complete
    products = await asyncio.gather(*tasks)
    
    # Process results
    for product in products:
        if product and 'variants' in product and product['variants']:
            variant = product['variants'][0]
            results.append({
                'Title': product['product_title'],
                'Current Price': variant['price_current'],
                'Original Price': variant['price_original'] if variant['price_original'] else 'None',
                'Has Discount': 'Yes' if variant['price_original'] else 'No'
            })
    
    # Display results in a table format
    print("\n\n" + "="*80)
    print("PRICE EXTRACTION TEST RESULTS")
    print("="*80)
    
    if results:
        # Print table header
        print(f"{'Title':<50} | {'Current Price':>12} | {'Original Price':>12} | {'Has Discount':>12}")
        print("-" * 90)
        
        # Print each row
        for result in results:
            print(f"{result['Title'][:50]:<50} | {result['Current Price']:>12} | {result['Original Price']:>12} | {result['Has Discount']:>12}")
        
        # Print summary
        discount_count = sum(1 for result in results if result['Has Discount'] == 'Yes')
        print("\nSummary:")
        print(f"Total products: {len(results)}")
        print(f"Products with discounts: {discount_count}")
        print(f"Products without discounts: {len(results) - discount_count}")
        
        # Save results to JSON for further analysis
        output_path = os.path.join(os.path.dirname(__file__), "price_test_results.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\nDetailed results saved to {output_path}")
    else:
        print("No results found.")

if __name__ == "__main__":
    try:
        asyncio.run(test_multiple_products())
    except Exception as e:
        print(f"Test error: {str(e)}")