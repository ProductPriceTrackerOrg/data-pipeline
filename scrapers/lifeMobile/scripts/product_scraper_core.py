"""
Core scraping logic for LifeMobile
"""
import scrapy
from scrapy.http import Request
import json
from datetime import datetime
import re
import random
import time
from urllib.parse import urlparse, urljoin
import logging
from typing import Dict, List, Set, Optional

from config.scraper_config import LifeMobileConfig
from models.product_models import Product, ProductVariant, ProductMetadata, PaymentOption

logger = logging.getLogger(__name__)


class LifeMobileSpider(scrapy.Spider):
    name = LifeMobileConfig.SPIDER_NAME
    allowed_domains = LifeMobileConfig.ALLOWED_DOMAINS
    start_urls = LifeMobileConfig.START_URLS
    custom_settings = LifeMobileConfig.get_scrapy_settings()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = LifeMobileConfig()
        self.products_collected = 0
        self.start_time = datetime.now()
        self.category_urls: Set[str] = set()
        self.product_urls: Set[str] = set()
        self.error_count = 0

    def normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicates with different parameters"""
        parsed = urlparse(url)
        # Remove common tracking parameters
        query_params = []
        for param in parsed.query.split('&'):
            if not any(p in param.lower() for p in ['utm_', 'fbclid', 'gclid', 'session', 'page']):
                query_params.append(param)
        
        clean_query = '&'.join(query_params)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}{'?' + clean_query if clean_query else ''}"
        return normalized

    def start_requests(self):
        """Generate initial requests"""
        for url in self.start_urls:
            yield Request(
                url,
                callback=self.parse_homepage,
                headers={"User-Agent": random.choice(self.config.USER_AGENTS)},
                errback=self.errback_handler
            )

    def parse_homepage(self, response):
        """Parse homepage to find category links"""
        # Check if we've exceeded maximum runtime
        if (datetime.now() - self.start_time).total_seconds() > self.config.MAX_RUNTIME:
            self.crawler.engine.close_spider(self, 'timeout')
            return
            
        # Extract all category URLs from the main navigation menu
        selectors = self.config.get_selectors()['category_links']

        for selector in selectors:
            category_links = response.css(selector).getall()
            for url in category_links:
                if url and "lifemobile.lk" in url:
                    full_url = self.normalize_url(urljoin(response.url, url))
                    if full_url not in self.category_urls:
                        self.category_urls.add(full_url)
                        yield Request(
                            url=full_url,
                            callback=self.parse_category,
                            priority=1,
                            headers={"User-Agent": random.choice(self.config.USER_AGENTS)},
                            errback=self.errback_handler,
                            meta={'depth': 1}
                        )

    def parse_category(self, response):
        """Parse category pages to find products and subcategories"""
        # Check runtime and product limits
        if (datetime.now() - self.start_time).total_seconds() > self.config.MAX_RUNTIME:
            self.crawler.engine.close_spider(self, 'timeout')
            return
            
        if response.status in [404, 503, 429] or self.products_collected >= self.config.MAX_PRODUCTS:
            self.error_count += 1
            return

        # Add small delay to prevent overwhelming the server
        time.sleep(0.1)
        
        # Track depth to prevent infinite pagination
        depth = response.meta.get('depth', 0) + 1
        if depth > self.config.MAX_PAGINATION_DEPTH:
            return

        selectors = self.config.get_selectors()

        # Extract subcategory URLs
        for selector in selectors['subcategory_links']:
            subcategory_links = response.css(selector).getall()
            for url in subcategory_links:
                if url and self.products_collected < self.config.MAX_PRODUCTS:
                    full_url = self.normalize_url(urljoin(response.url, url))
                    if full_url not in self.category_urls:
                        self.category_urls.add(full_url)
                        yield Request(
                            url=full_url,
                            callback=self.parse_category,
                            priority=1,
                            headers={"User-Agent": random.choice(self.config.USER_AGENTS)},
                            errback=self.errback_handler,
                            meta={'depth': depth}
                        )

        # Extract product URLs
        for selector in selectors['product_links']:
            product_links = response.css(selector).getall()
            for url in product_links:
                if (url and self.products_collected < self.config.MAX_PRODUCTS and 
                    "/product/" in url):
                    full_url = self.normalize_url(urljoin(response.url, url))
                    if full_url not in self.product_urls:
                        self.product_urls.add(full_url)
                        yield Request(
                            url=full_url,
                            callback=self.parse_product,
                            priority=2,
                            headers={"User-Agent": random.choice(self.config.USER_AGENTS)},
                            errback=self.errback_handler
                        )

        # Handle pagination
        if self.products_collected < self.config.MAX_PRODUCTS:
            for selector in selectors['pagination']:
                next_page = response.css(selector).get()
                if next_page:
                    full_url = self.normalize_url(urljoin(response.url, next_page))
                    if full_url not in self.category_urls:
                        self.category_urls.add(full_url)
                        yield Request(
                            url=full_url,
                            callback=self.parse_category,
                            priority=1,
                            headers={"User-Agent": random.choice(self.config.USER_AGENTS)},
                            errback=self.errback_handler,
                            meta={'depth': depth}
                        )

    def parse_product(self, response):
        """Parse individual product pages"""
        # Check runtime and product limits
        if (datetime.now() - self.start_time).total_seconds() > self.config.MAX_RUNTIME:
            self.crawler.engine.close_spider(self, 'timeout')
            return
            
        if response.status in [404, 503, 429] or self.products_collected >= self.config.MAX_PRODUCTS:
            self.error_count += 1
            return

        try:
            selectors = self.config.get_product_selectors()
            
            # Extract basic product information
            product_id = response.css(selectors['product_id']).get()
            if product_id:
                product_id = product_id.replace("product-", "")

            product_title = response.css(selectors['title']).get(default="").strip()
            description_html = response.css(selectors['description']).get()

            # Extract brand
            brand = response.css(selectors['brand_img']).get()
            if not brand:
                brand = response.css(selectors['brand_text']).get(default="").strip()

            # Extract category path
            category_path = response.css(selectors['category_path']).getall()[1:]

            # Extract images
            image_urls = response.css(selectors['images']).getall()

            # Extract pricing and availability
            availability = response.css(selectors['availability']).get(default="In stock").strip()
            price_current = response.css(selectors['price_current']).get(default="").strip()
            price_original = response.css(selectors['price_original']).get(default="").strip()
            discount_percentage = response.css(selectors['discount']).get()
            
            if discount_percentage:
                discount_percentage = discount_percentage.strip().replace("%", "")

            # Extract specifications
            specs = self._extract_specifications(response, selectors['specs'])

            # Extract payment options
            payment_options = self._extract_payment_options(response, selectors['payment_options'])

            # Extract variants
            variants = self._extract_variants(
                response, 
                selectors['variants_data'],
                product_id,
                price_current,
                price_original,
                discount_percentage,
                availability
            )

            # Create product data
            product_data = {
                "product_id_native": product_id,
                "product_url": response.url,
                "product_title": product_title,
                "description_html": description_html.strip() if description_html else None,
                "brand": brand if brand else None,
                "category_path": [cat.strip() for cat in category_path if cat.strip()],
                "specifications": specs,
                "image_urls": list(set(image_urls)),
                "variants": variants,
                "payment_options": payment_options,
                "metadata": {
                    "source_website": self.config.SHOP_CONTACT["website"],
                    "shop_contact_phone": self.config.SHOP_CONTACT["phone"],
                    "shop_contact_whatsapp": self.config.SHOP_CONTACT["whatsapp"],
                    "scrape_timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                },
            }

            self.products_collected += 1

            # Log progress
            if self.products_collected % 50 == 0:
                elapsed = datetime.now() - self.start_time
                rate = self.products_collected / elapsed.total_seconds()
                logger.info(
                    f"Progress: {self.products_collected} products | {rate:.2f} products/sec | Errors: {self.error_count}"
                )

            yield product_data

        except Exception as e:
            self.error_count += 1
            logger.error(f"Error parsing product: {str(e)}")

    def _extract_specifications(self, response, selector: str) -> Dict[str, str]:
        """Extract product specifications"""
        specs = {}
        spec_items = response.css(selector)
        for item in spec_items:
            text = item.css("::text").get(default="").strip()
            if ":" in text:
                key, value = text.split(":", 1)
                specs[key.strip()] = value.strip()
        return specs

    def _extract_payment_options(self, response, selector: str) -> List[Dict[str, str]]:
        """Extract payment options"""
        payment_options = []
        payment_elements = response.css(selector)
        for payment in payment_elements:
            payment_name = payment.css("img::attr(alt)").get(default="")
            payment_price = payment.css("span#discountPrice::text").get(default="").strip()
            if payment_name and payment_price:
                payment_options.append({
                    "provider": payment_name, 
                    "price": payment_price
                })
        return payment_options

    def _extract_variants(self, response, selector: str, product_id: str, 
                         price_current: str, price_original: str, 
                         discount_percentage: Optional[str], availability: str) -> List[Dict]:
        """Extract product variants"""
        variants_data = response.css(selector).get()
        variants = []

        if variants_data:
            try:
                variants_json = json.loads(variants_data)
                for variant in variants_json:
                    variant_id = variant.get("variation_id", "")
                    variant_title_parts = []

                    attributes = variant.get("attributes", {})
                    for attr_name, attr_value in attributes.items():
                        if attr_value:
                            attr_name_clean = (
                                attr_name.replace("attribute_", "")
                                .replace("pa_", "")
                                .replace("-", " ")
                                .title()
                            )
                            attr_value_clean = attr_value.replace("-", " ").title()
                            variant_title_parts.append(
                                f"{attr_name_clean}: {attr_value_clean}"
                            )

                    variant_title = (
                        " | ".join(variant_title_parts) if variant_title_parts else "Default"
                    )

                    variant_price_current = str(variant.get("display_price", ""))
                    variant_price_original = str(variant.get("display_regular_price", ""))

                    # Apply discount if available
                    if discount_percentage and variant_price_original:
                        try:
                            original_value = float(re.sub(r"[^\d.]", "", variant_price_original))
                            discount_value = float(discount_percentage)
                            discounted_value = original_value * (1 - discount_value / 100.0)

                            variant_price_original = str(original_value)
                            variant_price_current = str(round(discounted_value, 2))
                        except Exception:
                            variant_price_current = variant_price_current or variant_price_original
                            variant_price_original = variant_price_original or variant_price_current
                    else:
                        variant_price_current = variant_price_current or variant_price_original
                        variant_price_original = variant_price_original or variant_price_current

                    variants.append({
                        "variant_id_native": str(variant_id),
                        "variant_title": variant_title,
                        "price_current": variant_price_current,
                        "price_original": variant_price_original,
                        "currency": "Rs.",
                        "availability_text": availability,
                    })
            except json.JSONDecodeError:
                pass  # Fall back to default variant

        # Default variant if no variants found
        if not variants:
            # Apply discount to default prices if available
            if discount_percentage and price_original:
                try:
                    original_value = float(re.sub(r"[^\d.]", "", price_original))
                    discount_value = float(discount_percentage)
                    discounted_value = original_value * (1 - discount_value / 100.0)

                    price_original = str(original_value)
                    price_current = str(round(discounted_value, 2))
                except Exception:
                    price_current = price_current or price_original
                    price_original = price_original or price_current
            else:
                price_current = price_current or price_original
                price_original = price_original or price_current

            variants = [{
                "variant_id_native": product_id,
                "variant_title": "Default",
                "price_current": price_current,
                "price_original": price_original,
                "currency": "Rs.",
                "availability_text": availability,
            }]

        return variants

    def errback_handler(self, failure):
        """Handle request errors"""
        self.error_count += 1
        # Use the correct import for HttpError from spidermiddlewares
        from scrapy.spidermiddlewares.httperror import HttpError
        if failure.check(HttpError) and failure.value.response.status == 429:
            # Handle rate limiting if needed
            pass

    def closed(self, reason):
        """Called when spider closes"""
        elapsed = datetime.now() - self.start_time
        rate = self.products_collected / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0
        
        logger.info(f"Finished scraping. Total products: {self.products_collected}")
        logger.info(f"Total time: {elapsed.total_seconds():.2f} seconds")
        logger.info(f"Scraping rate: {rate:.2f} products/sec")
        logger.info(f"Total errors: {self.error_count}")
        logger.info(f"Reason: {reason}")