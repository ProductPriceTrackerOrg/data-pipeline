"""
Simple test script to scrape one product from CyberDeals
"""
import nest_asyncio
nest_asyncio.apply()

import asyncio
import httpx
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.product_scraper_core import AsyncCyberDealsScraper

async def test_single_product():
    """Test scraping a single product"""
    scraper = AsyncCyberDealsScraper(max_connections=1)
    
    # Test URL - we'll use a known product URL
    test_url = "https://cyberdeals.lk/product-category/computers/"
    
    print(f"🔍 Testing scraper with URL: {test_url}")
    
    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        # First, let's try to fetch the sitemap to get a real product URL
        print("📡 Fetching sitemap to get a real product URL...")
        sitemap_url = "https://cyberdeals.lk/sitemap_index.xml"
        
        try:
            sitemap_html = await scraper.fetch_page(client, sitemap_url)
            if sitemap_html:
                print("✅ Successfully fetched sitemap index")
                
                # Parse sitemap to get product sitemap URLs
                from selectolax.parser import HTMLParser
                tree = HTMLParser(sitemap_html)
                product_sitemaps = [
                    node.text() for node in tree.css("loc")
                    if "product-sitemap" in node.text()
                ]
                
                if product_sitemaps:
                    print(f"🔗 Found {len(product_sitemaps)} product sitemaps")
                    
                    # Get the first product sitemap
                    first_sitemap = product_sitemaps[0]
                    print(f"📄 Fetching first product sitemap: {first_sitemap}")
                    
                    sitemap_content = await scraper.fetch_page(client, first_sitemap)
                    if sitemap_content:
                        # Get first few product URLs
                        tree = HTMLParser(sitemap_content)
                        product_urls = [
                            node.text() for node in tree.css("url > loc")
                        ][:3]  # Just get first 3 products
                        
                        print(f"🎯 Found {len(product_urls)} product URLs to test")
                        
                        # Test scraping each product
                        for i, url in enumerate(product_urls, 1):
                            print(f"\n🔍 Testing product {i}: {url}")
                            
                            html = await scraper.fetch_page(client, url)
                            if html:
                                print(f"✅ Successfully fetched HTML ({len(html)} characters)")
                                
                                # Parse the product
                                product = scraper.parse_product_page(html, url)
                                if product:
                                    print(f"✅ Successfully parsed product:")
                                    print(f"   - Title: {product['product_title']}")
                                    print(f"   - Brand: {product['brand']}")
                                    print(f"   - Price: {product['variants'][0]['price_current']} {product['variants'][0]['currency']}")
                                    print(f"   - Categories: {product['category_path']}")
                                    print(f"   - Images: {len(product['image_urls'])} found")
                                    
                                    # Save this single product to test the save function
                                    scraper.save_data([product], "test_single_product.json")
                                    print("✅ Successfully saved test product!")
                                    return True
                                else:
                                    print("❌ Failed to parse product")
                            else:
                                print("❌ Failed to fetch product HTML")
                else:
                    print("❌ No product sitemaps found")
            else:
                print("❌ Failed to fetch sitemap index")
                
        except Exception as e:
            print(f"❌ Error during test: {e}")
            import traceback
            traceback.print_exc()
    
    return False

def test_run():
    """Run the test"""
    print("🚀 Starting CyberDeals scraper test...")
    result = asyncio.run(test_single_product())
    
    if result:
        print("\n🎉 Test completed successfully!")
    else:
        print("\n❌ Test failed!")
    
    return result

if __name__ == "__main__":
    test_run()
