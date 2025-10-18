#!/usr/bin/env python3
"""
Analyze the MacBook Air product page to see why category path is missing
"""
import sys
import os
import asyncio
import httpx
from selectolax.parser import HTMLParser

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper, DEFAULT_HEADERS

async def analyze_macbook_page():
    """Analyze why the MacBook Air page is missing category path"""
    url = "https://www.laptop.lk/index.php/product/apple-macbook-air-mgn63pa-a/"
    
    print(f"Analyzing product page: {url}")
    
    async with httpx.AsyncClient() as client:
        try:
            # Use the same headers as the main scraper
            response = await client.get(url, headers=DEFAULT_HEADERS, follow_redirects=True)
            if response.status_code != 200:
                print(f"Error: Got status code {response.status_code}")
                return
                
            html = response.text
            tree = HTMLParser(html)
            
            # Check for product container
            product_container = tree.css_first("div[id^=product-]")
            if not product_container:
                print("Error: Product container not found")
                return
                
            print(f"Product container found with ID: {product_container.id}")
            
            # Look for category elements
            category_elements = product_container.css("span.posted_in a")
            print(f"Category elements found: {len(category_elements)}")
            
            if category_elements:
                categories = [node.text(strip=True) for node in category_elements]
                print("Categories found:", categories)
            else:
                print("No category elements found in the expected location")
                
                # Check if product_meta div exists
                product_meta = product_container.css_first("div.product_meta")
                if product_meta:
                    print("Product meta div exists, but no categories found")
                    print("Content of product_meta:", product_meta.text(strip=True))
                else:
                    print("Product meta div not found")
                    
            # Check if there's any indication of categories elsewhere
            alt_categories = tree.css("a[rel='tag']")
            if alt_categories:
                print("Found alternative category elements:")
                for cat in alt_categories:
                    print(f"  - {cat.text(strip=True)}")
            
            # Check if this product has a price
            price_elements = tree.css("span.woocommerce-Price-amount")
            if price_elements:
                print("Price elements found:")
                for price in price_elements:
                    print(f"  - {price.text(strip=True)}")
            else:
                print("No price elements found")
                
            # Check if product is out of stock
            stock_status = product_container.css_first("p.stock")
            if stock_status:
                print(f"Stock status: {stock_status.text(strip=True)}")
            else:
                print("No stock status found")
                
        except Exception as e:
            print(f"Error analyzing page: {str(e)}")

if __name__ == "__main__":
    asyncio.run(analyze_macbook_page())