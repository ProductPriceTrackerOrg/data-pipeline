#!/usr/bin/env python3
"""
Detailed price extraction test - inspect HTML structure and extracted prices
"""
import sys
import os
import asyncio
import httpx
import json
import re
from selectolax.parser import HTMLParser

# Add the parent directory to the path to import the main scraper
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Import the scraper class
from main import AsyncLaptopLKScraper

# Sample product URLs - mix of products with and without discounts
TEST_PRODUCTS = [
    # Known product with discount
    "https://www.laptop.lk/index.php/product/logitech-shifter/",
    # Product without discount (to verify we correctly show None for original price)
    "https://www.laptop.lk/index.php/product/logitech-mx-master-3s/"
]

def inspect_price_elements(html):
    """Analyze HTML to see what price elements are present"""
    tree = HTMLParser(html)
    
    print("\n----- HTML PRICE ELEMENTS INSPECTION -----")
    
    # Check payment methods section
    payment_elements = tree.css("div.yith-wapo-block[data-block-name*='Payment Methods'] .yith-wapo-option-price .woocommerce-Price-amount")
    print(f"Payment method prices found: {len(payment_elements)}")
    for i, el in enumerate(payment_elements):
        price_text = el.text(strip=True)
        clean_price = re.sub(r'[^0-9.]', '', price_text)
        print(f"  Price {i+1}: {price_text} -> {clean_price}")
    
    # Check product actions section
    product_container = tree.css_first("div[id^=product-]")
    if product_container:
        # Check for sale/discount indicators
        sale_element = product_container.css_first("div.product-actions .price ins")
        print(f"Sale element ('ins') found: {'Yes' if sale_element else 'No'}")
        
        # Check for regular price
        curr_price = product_container.css_first("div.product-actions .price .woocommerce-Price-amount")
        if curr_price:
            print(f"Regular price found: {curr_price.text(strip=True)}")
        
        # Check for original price
        orig_price = product_container.css_first("div.product-actions .price del .woocommerce-Price-amount")
        if orig_price:
            print(f"Original price found: {orig_price.text(strip=True)}")
        else:
            print("No original price element found in product-actions")
            
            # Check alternate original price selectors
            alt_orig_price = product_container.css_first("p.price del .woocommerce-Price-amount, span.electro-price del .woocommerce-Price-amount")
            if alt_orig_price:
                print(f"Original price found (alternate selector): {alt_orig_price.text(strip=True)}")
    
    print("----------------------------------------\n")

async def test_price_extraction():
    """Test price extraction logic with detailed debugging"""
    scraper = AsyncLaptopLKScraper()
    
    for url in TEST_PRODUCTS:
        print(f"\n\n{'='*80}")
        print(f"TESTING: {url}")
        print(f"{'='*80}")
        
        async with httpx.AsyncClient() as client:
            # Fetch the product page
            html = await scraper.fetch_page(client, url)
            if not html:
                print("Failed to fetch page")
                continue
                
            # Inspect HTML price elements
            inspect_price_elements(html)
            
            # Parse the product using our scraper
            product_data = scraper.parse_product_data(html, url)
            if not product_data:
                print("Failed to parse product data")
                continue
                
            # Print the product information
            print(f"Product: {product_data['product_title']}")
            
            if product_data['variants']:
                variant = product_data['variants'][0]
                current_price = variant['price_current']
                original_price = variant['price_original']
                
                print(f"Current price: {current_price}")
                print(f"Original price: {original_price}")
                
                if original_price is None:
                    print("✓ Original price is None (no discount)")
                else:
                    print(f"✓ Original price is {original_price} (has discount)")
                    
            print(f"{'='*80}\n")

if __name__ == "__main__":
    asyncio.run(test_price_extraction())