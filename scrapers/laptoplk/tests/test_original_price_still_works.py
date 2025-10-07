#!/usr/bin/env python3
"""
Test that products with discounts still show the correct original price
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

async def test_original_price_still_works():
    """Test that products with discounts still show the correct original price"""
    # This product has a discount
    url = "https://www.laptop.lk/index.php/product/logitech-shifter/"
    
    print(f"Testing product with discount: {url}")
    
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
        
        # Check if original price is set correctly
        print(f"Product: {product_data['product_title']}")
        
        if product_data['variants']:
            variant = product_data['variants'][0]
            price_current = variant['price_current']
            price_original = variant['price_original']
            
            print(f"Current price: {price_current}")
            print(f"Original price: {price_original}")
            print(f"Original price type: {type(price_original).__name__}")
            
            if price_original == "32000.00":
                print("✅ Test PASSED: Original price is correctly set to '32000.00'")
            else:
                print(f"❌ Test FAILED: Original price should be '32000.00', but got {price_original}")
        
        return product_data

if __name__ == "__main__":
    asyncio.run(test_original_price_still_works())