"""
Core scraping functionality for CyberDeals
"""
import asyncio
import httpx
import re
from selectolax.parser import HTMLParser
from datetime import datetime
from typing import List, Dict, Optional, Any

try:
    from ..config.scraper_config import (
        DEFAULT_HEADERS, 
        MAX_RETRIES, 
        SHOP_METADATA, 
        KNOWN_BRANDS,
        DEFAULT_CURRENCY
    )
    from ..utils.scraper_utils import (
        extract_text_from_price, 
        detect_brand, 
        extract_image_urls,
        save_json
    )
except ImportError:
    # Fallback for direct execution
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from config.scraper_config import (
        DEFAULT_HEADERS, 
        MAX_RETRIES, 
        SHOP_METADATA, 
        KNOWN_BRANDS,
        DEFAULT_CURRENCY
    )
    from utils.scraper_utils import (
        extract_text_from_price, 
        detect_brand, 
        extract_image_urls,
        save_json
    )

class AsyncCyberDealsScraper:
    """Core scraper class for CyberDeals website"""
    
    def __init__(self, max_connections: int = 100, max_retries: int = MAX_RETRIES):
        self.source_website = SHOP_METADATA["source_website"]
        self.scrape_timestamp = datetime.now().isoformat()
        self.shop_phone = SHOP_METADATA["shop_contact_phone"]
        self.shop_whatsapp = SHOP_METADATA["shop_contact_whatsapp"]
        self.max_retries = max_retries
        self.headers = DEFAULT_HEADERS
        self.semaphore = asyncio.Semaphore(max_connections)
        self.known_brands = KNOWN_BRANDS

    async def fetch_page(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Fetch a page with retry logic"""
        async with self.semaphore:
            for attempt in range(self.max_retries):
                try:
                    resp = await client.get(url, headers=self.headers, timeout=30)
                    resp.raise_for_status()
                    return resp.text
                except (httpx.RequestError, httpx.HTTPStatusError):
                    if attempt + 1 == self.max_retries:
                        print(f"❌ Failed: {url}")
                    await asyncio.sleep(min(2 ** attempt, 5))
        return None

    def parse_product_page(self, html: str, url: str) -> Optional[Dict[str, Any]]:
        """Parse product details from HTML"""
        try:
            tree = HTMLParser(html)
            product_div = tree.css_first("div.product[id^=product-]")
            if not product_div:
                return None

            title = (tree.css_first("h1.product_title") or tree.css_first("h1.entry-title")).text(strip=True)
            product_id = product_div.attributes.get("id", "").split("-")[-1]

            desc_node = tree.css_first("div#tab-description, div.woocommerce-Tabs-panel--description")
            description_html = desc_node.html if desc_node else None

            category_nodes = tree.css("span.posted_in a")
            all_categories = [node.text(strip=True) for node in category_nodes]
            
            # Enhanced brand detection using precompiled list
            brand = detect_brand(title, all_categories, url, self.known_brands)
                
            # Optimized category path extraction
            category_path = []
            for c in all_categories:
                if not brand or brand.lower() not in c.lower():
                    category_path.append(c)

            # Enhanced image URL extraction with fallbacks
            image_urls = extract_image_urls(tree)

            price_current_node = tree.css_first("p.price ins .amount, p.price > .amount, span.price .amount")
            price_original_node = tree.css_first("p.price del .amount")
            price_current = extract_text_from_price(price_current_node)
            price_original = extract_text_from_price(price_original_node) if price_original_node else None

            stock = "Out of Stock" if tree.css_first("p.stock.out-of-stock") else "In Stock"

            return {
                "product_id_native": product_id,
                "product_url": url,
                "product_title": title,
                "description_html": description_html,
                "brand": brand,
                "category_path": category_path,
                "image_urls": image_urls,
                "variants": [{
                    "variant_id_native": product_id,
                    "variant_title": title,  # Use product title as variant title since there are no variants
                    "price_current": price_current,
                    "price_original": price_original,
                    "currency": DEFAULT_CURRENCY,
                    "availability_text": stock
                }],
                "metadata": {
                    "source_website": self.source_website,
                    "shop_contact_phone": self.shop_phone,
                    "shop_contact_whatsapp": self.shop_whatsapp,
                    "scrape_timestamp": self.scrape_timestamp
                }
            }
        except Exception as e:
            print(f"❌ Parsing Error @ {url}: {e}")
            return None

    def save_data(self, products: List[Dict[str, Any]], filename: str):
        """Save scraped products to a file"""
        save_json(products, filename)
