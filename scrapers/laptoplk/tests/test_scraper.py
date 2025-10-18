#!/usr/bin/env python3
"""
Test the updated laptop.lk scraper on a small set of products
"""
import sys
import os
import json
import asyncio
from selectolax.parser import HTMLParser
import httpx
import re

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper

async def test_specific_products():
    """Test the scraper on specific products to verify price extraction"""
    # URLs to test - known products with price issues
    test_urls = [
        "https://www.laptop.lk/index.php/product/viewsonic-27-ips-monitor/",
        "https://www.laptop.lk/index.php/product/eco-plus-cf500a-compatible-toner/",
        "https://www.laptop.lk/index.php/product/lenovo-ideacentre-3-07irb8-i7/"
    ]
    
    print(f"Testing {len(test_urls)} products with updated price extraction logic")
    
    # Initialize scraper
    scraper = AsyncLaptopLKScraper()
    
    results = []
    
    # Process each URL
    async with httpx.AsyncClient() as client:
        for url in test_urls:
            print(f"\nTesting URL: {url}")
            
            # Fetch the page
            html = await scraper.fetch_page(client, url)
            if not html:
                print(f"Failed to fetch {url}")
                continue
                
            # Parse the product data
            product_data = scraper.parse_product_data(html, url)
            if not product_data:
                print(f"Failed to parse product data for {url}")
                continue
                
            # Print extracted information
            print(f"Title: {product_data['product_title']}")
            if product_data['variants']:
                print(f"Current Price: {product_data['variants'][0]['price_current']}")
                print(f"Original Price: {product_data['variants'][0]['price_original']}")
                
            results.append(product_data)
    
    # Save results to file for inspection
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nTest results saved to {output_path}")
    
    return results

if __name__ == "__main__":
    asyncio.run(test_specific_products())