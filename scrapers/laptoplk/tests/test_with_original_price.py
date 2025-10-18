#!/usr/bin/env python3
"""
Test a product with an original price to verify our logic
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

async def test_product_with_original_price():
    """Test a product that should have an original price (discount)"""
    # This product has a discount and should have an original price
    url = "https://www.laptop.lk/index.php/product/logitech-shifter/"
    
    print(f"Testing product with original price: {url}")
    
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
            current_price = product_data['variants'][0]['price_current']
            original_price = product_data['variants'][0]['price_original']
            
            print(f"Current price: {current_price}")
            print(f"Original price: {original_price}")
            
            if original_price == "32000.00":
                print("✅ Test PASSED: Original price is correctly set to 32,000.00")
            else:
                print(f"❌ Test FAILED: Original price should be 32,000.00, but got {original_price}")
        
        return product_data

if __name__ == "__main__":
    asyncio.run(test_product_with_original_price())