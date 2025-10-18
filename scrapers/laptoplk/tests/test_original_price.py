#!/usr/bin/env python3
"""
Test the extraction of original (deleted) price in laptop.lk products
"""
import sys
import os
import re
from selectolax.parser import HTMLParser

def extract_price_from_text(price_text):
    """Extract just the number from the price text"""
    if not price_text:
        return None
    # Remove currency symbol and commas, keep just digits and decimal point
    clean_price = re.sub(r'[^0-9.]', '', price_text)
    return clean_price if clean_price else None

def analyze_price_elements(html_content):
    """Analyze all price elements in the HTML to find original (deleted) prices"""
    tree = HTMLParser(html_content)
    
    # First check if we have a product container
    product_container = tree.css_first("div[id^=product-]")
    if not product_container:
        print("No product container found!")
        return
    
    # Get the product title
    title_node = product_container.css_first("h1.product_title")
    title = title_node.text(strip=True) if title_node else "Unknown Product"
    print(f"Product: {title}\n")
    
    # Find all price elements
    price_elements = tree.css(".woocommerce-Price-amount")
    print(f"Found {len(price_elements)} price elements in the page")
    
    # Specifically look for del elements which contain original prices
    del_elements = tree.css("del")
    print(f"Found {len(del_elements)} del elements (potential original prices)")
    
    # Print all del elements with their content and parent classes
    print("\nOriginal price candidates (del elements):")
    for i, el in enumerate(del_elements):
        price_node = el.css_first(".woocommerce-Price-amount")
        if price_node:
            price_text = price_node.text(strip=True)
            clean_price = extract_price_from_text(price_text)
            parent_class = el.parent.attributes.get('class', '') if el.parent else 'no-parent'
            grandparent_class = el.parent.parent.attributes.get('class', '') if el.parent and el.parent.parent else 'no-grandparent'
            print(f"  {i+1}. Text: {price_text} -> {clean_price}")
            print(f"     Context: {grandparent_class} > {parent_class} > del")
            print(f"     HTML: {el.html}")
    
    # Look at price section to analyze structure
    price_section = product_container.css_first(".price")
    if price_section:
        print("\nPrice section structure:")
        print(f"HTML: {price_section.html}")
    
    # Test the extraction strategy from the main scraper
    print("\nTest extraction using main scraper method:")
    price_orig_node = product_container.css_first("p.price del .amount, span.electro-price del .amount, .summary .price del .woocommerce-Price-amount")
    if price_orig_node:
        price_text = price_orig_node.text(strip=True)
        clean_price = extract_price_from_text(price_text)
        print(f"Original price found: {price_text} -> {clean_price}")
    else:
        print("No original price found with current selectors")
    
    # Test improved extraction method
    print("\nTest extraction using improved method:")
    # First try product-actions section
    price_orig_node = product_container.css_first("div.product-actions .price del .woocommerce-Price-amount")
    if not price_orig_node:
        # Then try other common locations
        price_orig_node = product_container.css_first("p.price del .woocommerce-Price-amount, span.electro-price del .woocommerce-Price-amount, .summary .price del .woocommerce-Price-amount, del .woocommerce-Price-amount")
    
    if price_orig_node:
        price_text = price_orig_node.text(strip=True)
        clean_price = extract_price_from_text(price_text)
        print(f"Original price found: {price_text} -> {clean_price}")
    else:
        print("No original price found with improved selectors")

def main():
    """Main function to analyze original prices in HTML"""
    print("=" * 50)
    print("ANALYZING ORIGINAL PRICE EXTRACTION FOR LAPTOP.LK")
    print("=" * 50)
    
    # Read the HTML file
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_webpage.html")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        print("Please run download_page.py first to download a sample product page.")
        return 1
    
    with open(file_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    
    # Analyze the price elements
    analyze_price_elements(html_content)
    
    print("\n" + "=" * 50)
    print("ANALYSIS COMPLETE")
    print("=" * 50)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())