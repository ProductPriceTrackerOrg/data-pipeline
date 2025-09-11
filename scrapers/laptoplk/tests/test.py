import json
import asyncio
import httpx
import re
import time
from selectolax.parser import HTMLParser
from datetime import datetime
from typing import List, Dict, Optional, Set, Any
from tqdm.asyncio import tqdm


class AsyncCyberDealsScraper:
    def __init__(self, max_connections: int = 100, max_retries: int = 3):
        self.source_website = "cyberdeals.lk"
        self.scrape_timestamp = datetime.now().isoformat()
        self.shop_phone = "+94 77 842 1245"
        self.shop_whatsapp = "+94 77 842 1245"
        self.max_retries = max_retries
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
        }
        self.semaphore = asyncio.Semaphore(max_connections)

    async def fetch_page(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
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
            known_brands = ['dahua', 'imou', 'hikvision', 'ezviz', 'ruijie', 'reyee', 'jovision', 'zotac']
            brand = next((c for c in all_categories if c.lower() in known_brands), None)
            category_path = [c for c in all_categories if c.lower() != (brand or "").lower()]

            image_urls = list({
                node.attributes.get("href") for node in tree.css("div.woocommerce-product-gallery__image a")
                if node.attributes.get("href")
            })

            price_current_node = tree.css_first("p.price ins .amount, p.price > .amount, span.price .amount")
            price_original_node = tree.css_first("p.price del .amount")
            price_current = re.sub(r"[^\d.]", "", price_current_node.text(strip=True)) if price_current_node else "0"
            price_original = re.sub(r"[^\d.]", "", price_original_node.text(strip=True)) if price_original_node else None

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
                    "variant_title": "Default",
                    "price_current": price_current,
                    "price_original": price_original,
                    "currency": "LKR",
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
        output = {
            "extraction_info": {
                "total_products_extracted": len(products),
                "extraction_timestamp": self.scrape_timestamp
            },
            "products": products
        }
        with open(filename, "w", encoding="utfa-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Saved {len(products)} products to {filename}")


async def main():
    scraper = AsyncCyberDealsScraper()
    sitemap_url = "https://cyberdeals.lk/sitemap_index.xml"

    async with httpx.AsyncClient(http2=True, timeout=30) as client:
        print(f"🔍 Fetching sitemap index: {sitemap_url}")
        index_xml = await scraper.fetch_page(client, sitemap_url)
        if not index_xml:
            print("❌ Failed to load sitemap index.")
            return 0

        sitemap_urls = [
            node.text() for node in HTMLParser(index_xml).css("loc")
            if "product-sitemap" in node.text()
        ]

        sitemap_pages = await asyncio.gather(*[
            scraper.fetch_page(client, url) for url in sitemap_urls
        ])

        product_urls = {
            node.text() for xml in sitemap_pages if xml
            for node in HTMLParser(xml).css("url > loc")
        }

        print(f"🔗 Found {len(product_urls)} product URLs.")

        tasks = [scraper.fetch_page(client, url) for url in product_urls]
        html_pages = await tqdm.gather(*tasks, desc="📦 Downloading pages")

        parsed_products = [
            scraper.parse_product_page(html, url)
            for html, url in zip(html_pages, product_urls)
            if html
        ]

        products = [p for p in parsed_products if p]
        scraper.save_data(products, "cyberdeals_lk_scrape_optimized.json")
        return len(products)


if __name__ == "__main__":
    start_time = time.time()
    product_count = asyncio.run(main())
    end_time = time.time()

    print("\n" + "=" * 50)
    print("⚡ SCRAPE SUMMARY ⚡")
    print("=" * 50)
    print(f"✅ Total Products Scraped: {product_count}")
    print(f"⏱️ Total Time Taken: {end_time - start_time:.2f} seconds")
    print("=" * 50)
