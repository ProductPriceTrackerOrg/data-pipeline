#!/usr/bin/env python3
"""
Test the category path extraction for multiple products
"""
import sys
import os
import asyncio
import httpx
import json

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper

# Test URLs - one with standard category structure, one with alternative
TEST_URLS = [
    "https://www.laptop.lk/index.php/product/logitech-mouse-pad/",  # Standard structure
    "https://www.laptop.lk/index.php/product/apple-macbook-air-mgn63pa-a/"  # Alternative structure
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

async def test_category_extraction():
    """Test category extraction on multiple products"""
    results = []
    
    # Process each URL
    for url in TEST_URLS:
        product_data = await fetch_product_data(url)
        if product_data:
            print(f"\nProduct: {product_data['product_title']}")
            print(f"Brand: {product_data['brand']}")
            print(f"Category Path: {product_data['category_path']}")
            
            # Add to results
            results.append({
                'title': product_data['product_title'],
                'brand': product_data['brand'],
                'category_path': product_data['category_path'],
                'has_categories': len(product_data['category_path']) > 0
            })
    
    # Print summary
    print("\n" + "="*80)
    print("CATEGORY EXTRACTION TEST RESULTS")
    print("="*80)
    
    all_pass = True
    for result in results:
        status = "✅ PASS" if result['has_categories'] else "❌ FAIL"
        print(f"{status}: {result['title']} - Categories: {result['category_path']}")
        if not result['has_categories']:
            all_pass = False
    
    if all_pass:
        print("\n✅ All products have categories extracted successfully")
    else:
        print("\n❌ Some products still have missing categories")

if __name__ == "__main__":
    asyncio.run(test_category_extraction())