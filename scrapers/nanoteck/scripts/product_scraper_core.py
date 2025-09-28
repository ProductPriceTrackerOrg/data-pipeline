"""
Core product scraping implementation for Nanotek
"""

import asyncio
import logging
import re
import time
from typing import List, Dict, Any, Optional, Set
from datetime import datetime
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests

from models.product_models import (
    Product,
    ProductVariant,
    ProductMetadata,
    ScrapingStats,
)
from config.scraper_config import (
    BASE_URL,
    DEFAULT_HEADERS,
    MAX_WORKERS,
    BATCH_SIZE,
    SHOP_METADATA,
    DEFAULT_CURRENCY,
    SELENIUM_OPTIONS,
    TIMEOUT,
)
from utils.scraper_utils import filter_image_urls


class NanotekScraperCore:
    """Core Nanotek scraping functionality"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

        # Connection pooling for better performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=100, pool_maxsize=100, max_retries=3
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.scraping_stats = ScrapingStats(start_time=datetime.now())
        self.lock = threading.Lock()

    def get_categories(self) -> List[Dict[str, str]]:
        """Extract all categories from the website"""
        self.logger.info("🔍 Fetching categories from Nanotek.lk...")
        try:
            response = self.session.get(BASE_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")

            categories = []
            cat_elements = soup.find_all("li", class_="ty-catListItem")

            for cat in cat_elements:
                link = cat.find("a")
                if link and "href" in link.attrs:
                    category_url = urljoin(BASE_URL, link["href"])
                    category_name = link.text.strip()
                    if category_name:
                        categories.append(
                            {
                                "name": category_name[:100],  # Limit length
                                "url": category_url,
                            }
                        )

            self.logger.info(f"✅ Found {len(categories)} categories")
            return categories

        except Exception as e:
            self.logger.error(f"❌ Error fetching categories: {e}")
            return []

    def get_product_urls_from_category(self, category_url: str) -> List[str]:
        """Get all product URLs from a category with pagination"""
        product_urls = []
        page = 1
        max_pages = 100  # Safety limit

        self.logger.info(f"📄 Scraping category: {category_url}")

        while page <= max_pages:
            try:
                # Handle pagination
                if page == 1:
                    url = category_url
                else:
                    separator = "&" if "?" in category_url else "?"
                    url = f"{category_url}{separator}page={page}"

                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")

                # Find product links
                product_links = soup.find_all("a", href=re.compile(r"/product/"))

                if not product_links:
                    self.logger.info(
                        f"📄 No products found on page {page}, stopping pagination"
                    )
                    break

                page_products = 0
                for link in product_links:
                    href = link.get("href")
                    if href and "/product/" in href:
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in product_urls:
                            product_urls.append(full_url)
                            page_products += 1

                if page_products == 0:
                    break

                self.logger.debug(f"📄 Page {page}: Found {page_products} new products")

                # Check for next page
                next_page = soup.find("a", rel="next")
                if not next_page:
                    break

                page += 1
                time.sleep(1)  # Rate limiting

            except Exception as e:
                self.logger.error(f"❌ Error on page {page}: {e}")
                break

        self.logger.info(f"✅ Category complete: {len(product_urls)} products found")
        return product_urls

    def create_selenium_driver(self) -> webdriver.Chrome:
        """Create optimized Selenium Chrome driver"""
        chrome_options = Options()

        # Apply configuration from settings
        if SELENIUM_OPTIONS.get("headless"):
            chrome_options.add_argument("--headless")
        if SELENIUM_OPTIONS.get("no_sandbox"):
            chrome_options.add_argument("--no-sandbox")
        if SELENIUM_OPTIONS.get("disable_dev_shm_usage"):
            chrome_options.add_argument("--disable-dev-shm-usage")
        if SELENIUM_OPTIONS.get("disable_gpu"):
            chrome_options.add_argument("--disable-gpu")
        if SELENIUM_OPTIONS.get("disable_extensions"):
            chrome_options.add_argument("--disable-extensions")
        if SELENIUM_OPTIONS.get("disable_logging"):
            chrome_options.add_argument("--disable-logging")

        # Window size
        if SELENIUM_OPTIONS.get("window_size"):
            chrome_options.add_argument(
                f"--window-size={SELENIUM_OPTIONS['window_size']}"
            )

        # Performance optimizations
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-ipc-flooding-protection")

        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )

        return webdriver.Chrome(options=chrome_options)

    def scrape_product(self, product_url: str) -> Optional[Product]:
        """Scrape individual product data"""
        max_attempts = 2
        driver = None

        for attempt in range(max_attempts):
            try:
                driver = self.create_selenium_driver()
                driver.set_page_load_timeout(TIMEOUT)
                driver.get(product_url)

                # Wait for content
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, ".ty-productTitle")
                    )
                )
                time.sleep(1)

                soup = BeautifulSoup(driver.page_source, "html.parser")
                driver.quit()
                driver = None

                # Extract product data
                product_data = self.extract_product_data(soup, product_url)

                if product_data:
                    with self.lock:
                        self.scraping_stats.products_scraped += 1
                    return product_data
                else:
                    with self.lock:
                        self.scraping_stats.failed_scrapes += 1
                    return None

            except Exception as e:
                self.logger.debug(
                    f"❌ Error scraping {product_url} (attempt {attempt+1}): {str(e)[:100]}"
                )
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                    driver = None

                if attempt < max_attempts - 1:
                    time.sleep(1)

        # Log final failure
        with self.lock:
            self.scraping_stats.failed_scrapes += 1

        return None

    def extract_product_data(
        self, soup: BeautifulSoup, product_url: str
    ) -> Optional[Product]:
        """Extract structured product data from BeautifulSoup object"""
        try:
            # Product ID
            product_div = soup.find("div", attrs={"data-product-id": True})
            product_id = (
                product_div.get("data-product-id") if product_div else "unknown"
            )

            # Product title
            title_element = soup.find("h1", class_="ty-productTitle")
            product_title = (
                title_element.text.strip()[:200] if title_element else "Unknown Product"
            )

            # Description
            description_element = soup.find("div", class_="ty-productPage-info")
            description_html = (
                description_element.get_text()[:500] if description_element else ""
            )

            # Specifications
            specs = {}
            spec_tables = soup.select(".ty-productPage-info table")
            for table in spec_tables[:2]:
                rows = table.find_all("tr")
                for row in rows[:10]:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        key = cells[0].text.strip().replace(":", "")[:50]
                        value = cells[1].text.strip()[:100]
                        if key and value:
                            specs[key] = value

            # Images with filtering for valid formats
            image_elements = soup.select(".ty-sequence-slider img")
            image_urls = []
            for img in image_elements[:5]:
                src = img.get("src")
                if src and "data:" not in src:
                    full_url = urljoin(BASE_URL, src)
                    image_urls.append(full_url)

            # Filter to only include valid image formats (.webp, .jpg, .jpeg, .png)
            image_urls = filter_image_urls(image_urls)

            # Price extraction
            price_current, price_original = self.extract_prices(soup)

            # Create metadata
            metadata = ProductMetadata(
                source_website=SHOP_METADATA["source_website"],
                shop_contact_phone=SHOP_METADATA["shop_contact_phone"],
                shop_contact_whatsapp=SHOP_METADATA["shop_contact_whatsapp"],
                scrape_timestamp=datetime.now(),
            )

            # Create variant
            variant = ProductVariant(
                variant_id_native=product_id,
                variant_title="Default",
                price_current=price_current,
                price_original=price_original,
                currency=DEFAULT_CURRENCY,
                availability_text="In Stock",
            )

            # Create and return product
            product = Product(
                product_id_native=product_id,
                product_url=product_url,
                product_title=product_title,
                description_html=description_html.strip() if description_html else None,
                brand=None,  # Not available on Nanotek
                category_path=[],  # Will be populated by manager
                specifications=specs,
                image_urls=list(set(image_urls)),
                variants=[variant],
                metadata=metadata,
            )

            return product

        except Exception as e:
            self.logger.error(f"❌ Data extraction error for {product_url}: {e}")
            return None

    def extract_prices(self, soup: BeautifulSoup) -> tuple[float, Optional[float]]:
        """Extract current and original prices"""
        price_current = 0.0

        # Price selectors in order of preference
        price_selectors = [
            ".ty-price.ty-price-now",
            ".ty-productPage-price .ty-price.ty-price-now",
            ".ty-pay-price",
            ".ty-productPage-price",
        ]

        # Extract current price
        for selector in price_selectors:
            elements = soup.select(selector)
            if elements:
                for element in elements:
                    price_text = element.get_text(strip=True)
                    price_match = re.search(r"([\d,]+(?:\.\d{2})?)", price_text)
                    if price_match:
                        try:
                            price_str = price_match.group(1).replace(",", "")
                            price_current = float(price_str)
                            if price_current > 0:
                                break
                        except ValueError:
                            continue
            if price_current > 0:
                break

        # Extract original price (retail price)
        price_original = price_current  # Default fallback
        retail_element = soup.find("span", class_="ty-price ty-price-retail-price")
        if retail_element and retail_element.text.strip():
            try:
                retail_text = (
                    retail_element.text.strip().replace(",", "").replace("\u202f", "")
                )
                price_match = re.search(r"([\d.]+)", retail_text)
                if price_match:
                    price_original = float(price_match.group(1))
            except (ValueError, AttributeError):
                pass

        return price_current, price_original

    def get_all_product_urls(self) -> List[str]:
        """Discover all product URLs from all categories"""
        self.logger.info("🔍 Starting product URL discovery...")
        categories = self.get_categories()

        if not categories:
            self.logger.error("❌ No categories found!")
            return []

        all_product_urls = []

        # Process each category
        for i, category in enumerate(categories, 1):
            self.logger.info(
                f"📂 Processing category {i}/{len(categories)}: {category['name']}"
            )
            product_urls = self.get_product_urls_from_category(category["url"])
            all_product_urls.extend(product_urls)
            time.sleep(2)  # Rate limiting between categories

        # Remove duplicates
        all_product_urls = list(set(all_product_urls))
        self.scraping_stats.total_urls_discovered = len(all_product_urls)
        self.logger.info(
            f"🎯 Total unique products discovered: {len(all_product_urls)}"
        )

        return all_product_urls

    def scrape_products_batch(self, product_urls: List[str]) -> List[Product]:
        """Scrape a batch of product URLs"""
        products = []
        self.logger.info(f"🔥 Processing batch: {len(product_urls)} products")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self.scrape_product, url): url for url in product_urls
            }

            completed = 0
            for future in as_completed(future_to_url):
                try:
                    product_data = future.result()
                    if product_data:
                        products.append(product_data)

                    completed += 1
                    if completed % 5 == 0:
                        progress = (completed / len(product_urls)) * 100
                        self.logger.info(
                            f"📊 Batch progress: {progress:.1f}% ({completed}/{len(product_urls)})"
                        )

                except Exception as e:
                    self.logger.error(f"❌ Future processing error: {e}")

        self.logger.info(f"✅ Batch completed: {len(products)} valid products")
        return products

    def update_final_stats(self):
        """Update final scraping statistics"""
        self.scraping_stats.end_time = datetime.now()
        self.scraping_stats.duration_seconds = (
            self.scraping_stats.end_time - self.scraping_stats.start_time
        ).total_seconds()

        total_attempts = (
            self.scraping_stats.products_scraped + self.scraping_stats.failed_scrapes
        )
        if total_attempts > 0:
            self.scraping_stats.success_rate = (
                self.scraping_stats.products_scraped / total_attempts
            ) * 100
