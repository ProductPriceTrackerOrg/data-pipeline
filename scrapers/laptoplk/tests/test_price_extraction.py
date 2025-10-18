#!/usr/bin/env python3
"""
Test improved price extraction for laptop.lk scraper
"""
import sys
import os
import re
import json
from selectolax.parser import HTMLParser
import httpx

def extract_price_from_text(price_text):
    """Extract just the number from the price text"""
    if not price_text:
        return None
    # Remove currency symbol and commas, keep just digits and decimal point
    clean_price = re.sub(r'[^\d.]', '', price_text)
    return clean_price if clean_price else None

def debug_price_elements(tree):
    """Debug function to find all price elements in the document"""
    all_price_elements = tree.css(".woocommerce-Price-amount")
    print(f"Found {len(all_price_elements)} price elements in the page")
    
    # Print the first few with their text content
    if all_price_elements:
        print("Sample price elements:")
        for i, el in enumerate(all_price_elements[:5]):  # Show up to 5 examples
            parent_class = el.parent.attributes.get('class', '') if el.parent else 'no-parent'
            grandparent_class = el.parent.parent.attributes.get('class', '') if el.parent and el.parent.parent else 'no-grandparent'
            price_text = el.text(strip=True)
            clean_price = extract_price_from_text(price_text)
            print(f"  {i+1}. Text: {price_text} -> {clean_price}")
            print(f"     Context: {grandparent_class} > {parent_class}")

def extract_prices_new_method(html_content):
    """
    Extract prices using improved selectors for the new website structure
    Returns (current_price, original_price) tuple
    """
    tree = HTMLParser(html_content)
    
    # Debug - Find all price elements in the document
    debug_price_elements(tree)
    
    # Check several potential locations for prices
    current_price = None
    original_price = None
    
    # 1. Check payment methods section first (most reliable for current actual price)
    payment_elements = tree.css("div.yith-wapo-block[data-block-name*='Payment Methods'] .yith-wapo-option-price .woocommerce-Price-amount")
    if payment_elements:
        prices = []
        for el in payment_elements:
            price_text = el.text(strip=True)
            clean_price = extract_price_from_text(price_text)
            if clean_price:
                prices.append(clean_price)
        
        if prices:
            print(f"Found payment method prices: {prices}")
            # Use the lowest price (usually the cash price)
            current_price = min(prices)
    
    # 2. Check product actions section
    if not current_price:
        price_element = tree.css_first("div.product-actions .price .woocommerce-Price-amount")
        if price_element:
            price_text = price_element.text(strip=True)
            current_price = extract_price_from_text(price_text)
            print(f"Found product-actions price: {price_text} -> {current_price}")
    
    # 3. Check summary section
    if not current_price:
        price_element = tree.css_first(".summary .price .woocommerce-Price-amount")
        if price_element:
            price_text = price_element.text(strip=True)
            current_price = extract_price_from_text(price_text)
            print(f"Found summary price: {price_text} -> {current_price}")
    
    # 4. Try original selectors as last resort
    if not current_price:
        price_element = tree.css_first("p.price ins .amount, span.electro-price ins .amount, p.price > .amount, span.electro-price > .amount")
        if price_element:
            price_text = price_element.text(strip=True)
            current_price = extract_price_from_text(price_text)
            print(f"Found price with original selectors: {price_text} -> {current_price}")
    
    # 5. Absolute last resort - any price element
    if not current_price:
        price_element = tree.css_first(".woocommerce-Price-amount")
        if price_element:
            price_text = price_element.text(strip=True)
            current_price = extract_price_from_text(price_text)
            print(f"Found price with generic selector: {price_text} -> {current_price}")
    
    # Look for original (deleted) price
    orig_element = tree.css_first("p.price del .woocommerce-Price-amount, span.electro-price del .woocommerce-Price-amount, .summary .price del .woocommerce-Price-amount, div.product-actions .price del .woocommerce-Price-amount")
    if orig_element:
        price_text = orig_element.text(strip=True)
        original_price = extract_price_from_text(price_text)
        print(f"Found original price: {price_text} -> {original_price}")
    
    return current_price, original_price

def extract_prices_old_method(html_content):
    """
    Extract prices using the current method in the scraper
    Returns (current_price, original_price) tuple
    """
    tree = HTMLParser(html_content)
    product_container = tree.css_first("div[id^=product-]")
    
    if not product_container:
        print("No product container found")
        return None, None
    
    # Current method from scraper
    price_curr_node = product_container.css_first("p.price ins .amount, span.electro-price ins .amount, p.price > .amount, span.electro-price > .amount")
    price_orig_node = product_container.css_first("p.price del .amount, span.electro-price del .amount")
    
    price_current = re.sub(r'[^\d.]', '', price_curr_node.text(strip=True)) if price_curr_node else "0"
    price_original = re.sub(r'[^\d.]', '', price_orig_node.text(strip=True)) if price_orig_node else None
    
    print(f"Old method found current price: {price_current}")
    print(f"Old method found original price: {price_original}")
    
    return price_current, price_original

def test_price_extraction_with_local_file():
    """Test price extraction from a local HTML file"""
    try:
        # Try to read the temporary file that should be in the current directory
        file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp_webpage.html")
        
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            print("Downloading a sample page first...")
            from download_page import download_sample_page
            file_path = download_sample_page()
            if not file_path:
                return None, None, None
        
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        # Extract product ID from HTML if possible
        tree = HTMLParser(html_content)
        product_container = tree.css_first("div[id^=product-]")
        if product_container and product_container.id:
            product_id = product_container.id.split('-')[-1]
            print(f"Product ID: {product_id}")
        else:
            product_id = "Unknown"
            print("Could not extract product ID")
        
        # Test old method
        print("\n--- Testing OLD price extraction method ---")
        old_current, old_original = extract_prices_old_method(html_content)
        
        # Test new method
        print("\n--- Testing NEW price extraction method ---")
        new_current, new_original = extract_prices_new_method(html_content)
        
        # Compare results
        print("\n--- Comparison ---")
        print(f"OLD method: Current={old_current}, Original={old_original}")
        print(f"NEW method: Current={new_current}, Original={new_original}")
        
        # The difference is significant if the prices differ by more than 5%
        if old_current and new_current:
            old_price = float(old_current)
            new_price = float(new_current)
            if abs(old_price - new_price) / max(old_price, new_price) > 0.05:
                print("⚠️ SIGNIFICANT PRICE DIFFERENCE DETECTED!")
            else:
                print("✅ Prices are similar.")
        
        return new_current, new_original, product_id
        
    except Exception as e:
        print(f"Error testing price extraction: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

def test_with_product_url(url):
    """Test price extraction from a live product URL"""
    try:
        # Fetch the product page
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = httpx.get(url, headers=headers, follow_redirects=True, timeout=30)
        response.raise_for_status()
        html_content = response.text
        
        print(f"\n--- Testing URL: {url} ---")
        
        # Test old method
        print("\n--- Testing OLD price extraction method ---")
        old_current, old_original = extract_prices_old_method(html_content)
        
        # Test new method
        print("\n--- Testing NEW price extraction method ---")
        new_current, new_original = extract_prices_new_method(html_content)
        
        # Compare results
        print("\n--- Comparison ---")
        print(f"OLD method: Current={old_current}, Original={old_original}")
        print(f"NEW method: Current={new_current}, Original={new_original}")
        
        return new_current, new_original
    
    except Exception as e:
        print(f"Error fetching or processing URL: {e}")
        return None, None

def check_prices_in_json_sample():
    """Check a few products from the existing JSON file to compare prices"""
    try:
        # Look for the JSON file in multiple potential locations
        possible_paths = [
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scraped_data", "laptop_lk_scrape.json"),
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scraped_data", "laptoplk_products.json"),
            os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "scraped_data", "laptop_lk_scrape.json")
        ]
        
        json_path = None
        for path in possible_paths:
            if os.path.exists(path):
                json_path = path
                break
        
        if not json_path:
            print("No JSON file found at expected locations")
            return
        
        with open(json_path, "r", encoding="utf-8") as f:
            products = json.load(f)
        
        print(f"Loaded {len(products)} products from JSON")
        
        # Test a few random products
        test_products = [
            products[0],  # First product
            products[len(products) // 2],  # Middle product
            products[-1]  # Last product
        ]
        
        for i, product in enumerate(test_products):
            if not product.get("product_url") or not product.get("variants"):
                continue
                
            print(f"\nProduct {i+1}: {product.get('product_title', 'Unknown')}")
            print(f"URL: {product['product_url']}")
            
            variant = product['variants'][0] if product['variants'] else {}
            print(f"Current JSON price: {variant.get('price_current', 'Unknown')}")
            print(f"Original JSON price: {variant.get('price_original', 'Unknown')}")
            
            # Test with live URL
            print("Fetching live price...")
            current_price, original_price = test_with_product_url(product['product_url'])
            
            if current_price and variant.get('price_current') and current_price != variant['price_current']:
                print(f"❌ Price mismatch: JSON has {variant['price_current']}, live site has {current_price}")
            else:
                print("✅ Prices match or could not verify")
                
    except Exception as e:
        print(f"Error checking JSON prices: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main test function"""
    print("=" * 50)
    print("TESTING PRICE EXTRACTION FOR LAPTOP.LK")
    print("=" * 50)
    
    # Test with local file first
    print("\n1. Testing with local HTML file:")
    current_price, original_price, product_id = test_price_extraction_with_local_file()
    
    # Check a few products from the JSON file
    print("\n2. Checking prices in JSON sample:")
    check_prices_in_json_sample()
    
    # Additional URLs to test if provided as command line arguments
    if len(sys.argv) > 1:
        for url in sys.argv[1:]:
            test_with_product_url(url)
    
    print("\n" + "=" * 50)
    print("TEST COMPLETE")
    print("=" * 50)
    
    # Return recommended code to replace the price extraction in main.py
    print("\nRECOMMENDED CODE TO FIX PRICE EXTRACTION:")
    print("=" * 50)
    print("""
# Extract prices - check payment methods section first (most reliable for actual price)
payment_elements = tree.css("div.yith-wapo-block[data-block-name*='Payment Methods'] .yith-wapo-option-price .woocommerce-Price-amount")
if payment_elements:
    prices = []
    for el in payment_elements:
        price_text = el.text(strip=True)
        clean_price = re.sub(r'[^\d.]', '', price_text)
        if clean_price:
            prices.append(clean_price)
    
    if prices:
        price_current = min(prices)  # Use lowest price (usually the cash price)
    else:
        # Fallback to product actions section
        price_curr_node = product_container.css_first("div.product-actions .price .woocommerce-Price-amount")
        price_current = re.sub(r'[^\d.]', '', price_curr_node.text(strip=True)) if price_curr_node else "0"
else:
    # Fallback to product actions section
    price_curr_node = product_container.css_first("div.product-actions .price .woocommerce-Price-amount")
    if not price_curr_node:
        # Fallback to original selectors
        price_curr_node = product_container.css_first("p.price ins .amount, span.electro-price ins .amount, p.price > .amount, span.electro-price > .amount, .summary .price .woocommerce-Price-amount")
    price_current = re.sub(r'[^\d.]', '', price_curr_node.text(strip=True)) if price_curr_node else "0"

# Look for original (deleted) price
price_orig_node = product_container.css_first("div.product-actions .price del .woocommerce-Price-amount")
if not price_orig_node:
    price_orig_node = product_container.css_first("p.price del .amount, span.electro-price del .amount, .summary .price del .woocommerce-Price-amount")
price_original = re.sub(r'[^\d.]', '', price_orig_node.text(strip=True)) if price_orig_node else None
""")
    print("=" * 50)
    
    return 0

if __name__ == "__main__":
    main()
