"""Test case for products with no price displayed

This test verifies that products without prices have "null" for price_current
instead of "0" in the output JSON.

This test uses a mock HTML with a missing price to simulate a product page without a price.
"""

import json
import os
import sys

# Add the parent directory to the path so we can import from there
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import AsyncLaptopLKScraper

def test_missing_price():
    """Test a product that doesn't have a price using mock HTML"""
    url = "https://www.laptop.lk/index.php/product/sample-product-no-price/"
    
    # Create mock HTML without price elements
    html_content = """
    <html>
    <body>
        <div id="product-12345" class="product">
            <h1 class="product_title">Sample Product Without Price</h1>
            <div class="product-actions">
                <!-- No price element here -->
                <div class="stock in-stock">In Stock</div>
            </div>
            <div class="woocommerce-tabs">
                <div id="tab-description">
                    <p>This is a sample product description.</p>
                </div>
            </div>
            <div class="product_meta">
                <span class="posted_in">Categories: 
                    <a href="/product-category/test" rel="tag">Test Category</a>
                </span>
            </div>
        </div>
    </body>
    </html>
    """
    
    print("Creating test scraper instance...")
    scraper = AsyncLaptopLKScraper()
    
    print("Processing product data...")
    product_data = scraper.parse_product_data(html_content, url)
    
    # Print the extracted data for inspection
    print("\nExtracted product data:")
    formatted_json = json.dumps(product_data, indent=2)
    print(formatted_json)
    
    # Check if the current price is "null" instead of "0"
    price_current = product_data["variants"][0]["price_current"]
    print(f"\nExtracted current price: {price_current}")
    assert price_current == "null", f"Expected price to be 'null', but got {price_current}"
    
    print("\n✅ Test passed! Product with no price correctly has 'null' for price_current.")

if __name__ == "__main__":
    test_missing_price()