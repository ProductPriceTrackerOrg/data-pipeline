#!/usr/bin/env python3
"""
Test specifically for the null original price handling
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

async def test_null_original_price():
    """Test that original price is set to "null" string when none exists"""
    # This product has no discount
    url = "https://www.laptop.lk/index.php/product/logitech-mx-master-3s-wireless-mouse/"
    
    print(f"Testing product without discount: {url}")
    
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
        
        # Check if original price is set to "null" string
        print(f"Product: {product_data['product_title']}")
        
        if product_data['variants']:
            variant = product_data['variants'][0]
            price_current = variant['price_current']
            price_original = variant['price_original']
            
            print(f"Current price: {price_current}")
            print(f"Original price: {price_original}")
            print(f"Original price type: {type(price_original).__name__}")
            
            if price_original == "null":
                print("✅ Test PASSED: Original price is correctly set to 'null' string")
            else:
                print(f"❌ Test FAILED: Original price should be 'null' string, but got {price_original}")
        
        return product_data

if __name__ == "__main__":
    asyncio.run(test_null_original_price())