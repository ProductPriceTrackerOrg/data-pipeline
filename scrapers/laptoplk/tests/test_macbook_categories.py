#!/usr/bin/env python3
"""
Test the category path extraction for the MacBook Air product
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

async def test_macbook_categories():
    """Test that the MacBook Air product now correctly extracts categories"""
    url = "https://www.laptop.lk/index.php/product/apple-macbook-air-mgn63pa-a/"
    
    print(f"Testing category extraction for: {url}")
    
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
        
        # Check the category path and brand
        print(f"Product: {product_data['product_title']}")
        print(f"Brand: {product_data['brand']}")
        print(f"Category Path: {product_data['category_path']}")
        
        # Verify that category path is not empty
        if product_data['category_path']:
            print("✅ Test PASSED: Category path is now correctly extracted")
        else:
            print("❌ Test FAILED: Category path is still empty")
            
        # Save the updated product data to JSON for inspection
        output_path = os.path.join(os.path.dirname(__file__), "macbook_with_categories.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(product_data, f, indent=2, ensure_ascii=False)
        print(f"Product data saved to: {output_path}")
        
        return product_data

if __name__ == "__main__":
    asyncio.run(test_macbook_categories())