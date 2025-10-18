#!/usr/bin/env python3
"""
Test a product without original price to verify our logic
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

async def test_product_without_original_price():
    """Test a product that should not have an original price"""
    # This is a product that likely doesn't have an original price (no discount)
    url = "https://www.laptop.lk/index.php/product/logitech-mouse-pad/"
    
    print(f"Testing product without original price: {url}")
    
    # Initialize scraper
    scraper = AsyncLaptopLKScraper()
    
    # Fetch and parse the product
    async with httpx.AsyncClient() as client:
        html = await scraper.fetch_page(client, url)
        if not html:
            print("Failed to fetch product page")
            return None
        
        product_data = scraper.parse_product_data(html, url)
        if not product_data:
            print("Failed to parse product data")
            return None
        
        # Check if original price is None (as expected)
        print(f"Product: {product_data['product_title']}")
        
        if product_data['variants']:
            current_price = product_data['variants'][0]['price_current']
            original_price = product_data['variants'][0]['price_original']
            
            print(f"Current price: {current_price}")
            print(f"Original price: {original_price}")
            
            if original_price is None:
                print("✅ Test PASSED: Original price is correctly set to None")
            else:
                print(f"❌ Test FAILED: Original price should be None, but got {original_price}")
        
        return product_data

if __name__ == "__main__":
    asyncio.run(test_product_without_original_price())