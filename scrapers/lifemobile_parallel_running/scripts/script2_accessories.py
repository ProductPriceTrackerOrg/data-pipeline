import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
import json
from datetime import datetime
import re
import random
import time
from urllib.parse import urlparse, urljoin
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class LifeMobileSpider(scrapy.Spider):
    name = "lifemobile_script2"
    allowed_domains = ["lifemobile.lk"]
    start_urls = [
        "https://lifemobile.lk/product-category/accessories/",
        "https://lifemobile.lk/product-category/phone-accessories/",
        "https://lifemobile.lk/product-category/cases/",
        "https://lifemobile.lk/product-category/chargers/",
    ]

    # Rotating user agents
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    ]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.25,
        "RETRY_TIMES": 1,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 429],
        "DOWNLOAD_TIMEOUT": 15,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.25,
        "AUTOTHROTTLE_MAX_DELAY": 3,
        "HTTPCACHE_ENABLED": False,
        "DNSCACHE_ENABLED": True,
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": False,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
    }

    def __init__(self):
        self.products_collected = 0
        self.start_time = datetime.now()
        self.category_urls = set()
        self.product_urls = set()
        self.error_count = 0
        self.max_products = 10000
        # Reduced for testing
        self.max_runtime = 120  # 1 hour maximum runtime

    def filter_valid_image_urls(self, image_urls):
        """Filter image URLs to only include valid webp, jpg, png formats"""
        valid_urls = []
        valid_extensions = (".webp", ".jpg", ".jpeg", ".png")

        for url in image_urls:
            if not url or url.strip() == "":
                continue

            # Skip SVG placeholders and invalid URLs
            if "data:image/svg+xml" in url:
                continue
            if "woocommerce-placeholder.png" in url:
                continue
            if url.startswith("data:"):
                continue

            # Check if URL ends with valid image extension
            url_lower = url.lower()
            if any(url_lower.endswith(ext) for ext in valid_extensions):
                valid_urls.append(url)
            # Also include URLs that contain valid extensions (for query params)
            elif any(ext in url_lower for ext in valid_extensions):
                valid_urls.append(url)
            # Check for lifemobile domain images without clear extension
            elif "lifemobile.lk/wp-content/uploads/" in url and not url.endswith("/"):
                valid_urls.append(url)

        return valid_urls

    def normalize_url(self, url):
        """Normalize URL to prevent duplicates with different parameters"""
        parsed = urlparse(url)
        # Remove common tracking parameters
        query_params = []
        for param in parsed.query.split("&"):
            if not any(
                p in param.lower()
                for p in ["utm_", "fbclid", "gclid", "session", "page"]
            ):
                query_params.append(param)

        clean_query = "&".join(query_params)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}{'?' + clean_query if clean_query else ''}"
        return normalized

    def start_requests(self):
        for url in self.start_urls:
            yield Request(
                url,
                callback=self.parse_category,
                headers={"User-Agent": random.choice(self.USER_AGENTS)},
                errback=self.errback_handler,
            )

    def parse_category(self, response):
        # Check if we've exceeded maximum runtime
        if (datetime.now() - self.start_time).total_seconds() > self.max_runtime:
            self.crawler.engine.close_spider(self, "timeout")
            return

        if (
            response.status in [404, 503, 429]
            or self.products_collected >= self.max_products
        ):
            self.error_count += 1
            return

        # Track depth to prevent infinite pagination
        depth = response.meta.get("depth", 0) + 1
        if depth > 10:  # Limit pagination depth
            return

        # Extract subcategory URLs
        subcategory_links = response.css(
            "ul.product-categories li a::attr(href), "
            ".widget_product_categories a::attr(href), "
            "div.product-category a::attr(href)"
        ).getall()

        for url in subcategory_links:
            if url and self.products_collected < self.max_products:
                full_url = self.normalize_url(urljoin(response.url, url))
                if full_url not in self.category_urls:
                    self.category_urls.add(full_url)
                    yield Request(
                        url=full_url,
                        callback=self.parse_category,
                        priority=1,
                        headers={"User-Agent": random.choice(self.USER_AGENTS)},
                        errback=self.errback_handler,
                        meta={"depth": depth},
                    )

        # Extract product URLs
        product_links = response.css(
            "a.woocommerce-LoopProduct-link::attr(href), "
            "h2.woocommerce-loop-product__title a::attr(href), "
            "li.product a::attr(href)"
        ).getall()

        for url in product_links:
            if (
                url
                and self.products_collected < self.max_products
                and "/product/" in url
            ):
                full_url = self.normalize_url(urljoin(response.url, url))
                if full_url not in self.product_urls:
                    self.product_urls.add(full_url)
                    yield Request(
                        url=full_url,
                        callback=self.parse_product,
                        priority=2,
                        headers={"User-Agent": random.choice(self.USER_AGENTS)},
                        errback=self.errback_handler,
                    )

        # Handle pagination
        if self.products_collected < self.max_products:
            next_page = response.css(
                "a.next::attr(href), " "a.next.page-numbers::attr(href)"
            ).get()

            if next_page:
                full_url = self.normalize_url(urljoin(response.url, next_page))
                if full_url not in self.category_urls:
                    self.category_urls.add(full_url)
                    yield Request(
                        url=full_url,
                        callback=self.parse_category,
                        priority=1,
                        headers={"User-Agent": random.choice(self.USER_AGENTS)},
                        errback=self.errback_handler,
                        meta={"depth": depth},
                    )

    def parse_product(self, response):
        # Check if we've exceeded maximum runtime
        if (datetime.now() - self.start_time).total_seconds() > self.max_runtime:
            self.crawler.engine.close_spider(self, "timeout")
            return

        if (
            response.status in [404, 503, 429]
            or self.products_collected >= self.max_products
        ):
            self.error_count += 1
            return

        try:
            product_id = response.css("div.product::attr(id)").get()
            if product_id:
                product_id = product_id.replace("product-", "")

            product_title = (
                response.css("h1.product_title::text").get(default="").strip()
            )
            description_html = response.css(
                "div.woocommerce-product-details__short-description"
            ).get()

            brand = response.css("div.brand img::attr(alt)").get()
            if not brand:
                brand = response.css("span.posted_in a::text").get(default="").strip()

            category_path = response.css(
                "span.posted_in a::text, nav.woocommerce-breadcrumb a::text"
            ).getall()[
                1:
            ]  # Skip the first element (usually "Home")

            image_urls = response.css(
                "div.woocommerce-product-gallery img::attr(src), "
                "div.woocommerce-product-gallery img::attr(data-src), "
                "img.wp-post-image::attr(src)"
            ).getall()

            # Filter to only valid image formats
            image_urls = self.filter_valid_image_urls(image_urls)

            # Remove duplicates while preserving order
            seen = set()
            image_urls = [
                url for url in image_urls if not (url in seen or seen.add(url))
            ]

            availability = response.css("p.stock::text").get(default="In stock").strip()

            # Price extraction
            price_current = response.css(
                "p.price ins span.amount bdi::text, " "p.price span.amount bdi::text"
            ).get(default="")

            price_original = response.css("p.price del span.amount bdi::text").get(
                default=""
            )

            # Clean prices
            def clean_price(price_str):
                if not price_str:
                    return ""
                # Remove currency symbols and commas
                return re.sub(r"[^\d.]", "", price_str.strip())

            price_current = clean_price(price_current)
            price_original = clean_price(price_original)

            # If no current price but original exists, use original
            if not price_current and price_original:
                price_current = price_original
                price_original = ""

            # If neither price found, try alternative selectors
            if not price_current:
                price_text = response.css("p.price::text").get(default="")
                if price_text:
                    price_match = re.search(r"(\d+\.?\d*)", price_text)
                    if price_match:
                        price_current = price_match.group(1)

            # Extract specifications
            specs = {}
            spec_items = response.css(
                "div.woocommerce-product-details__short-description li, "
                "div.aps-feature-info div"
            )
            for item in spec_items:
                text = item.css("::text").get(default="").strip()
                if ":" in text:
                    key, value = text.split(":", 1)
                    specs[key.strip()] = value.strip()

            # Process variants
            variants_data = response.css(
                "form.variations_form::attr(data-product_variations)"
            ).get()
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
                            " | ".join(variant_title_parts)
                            if variant_title_parts
                            else "Default"
                        )

                        variant_price_current = clean_price(
                            str(variant.get("display_price", ""))
                        )
                        variant_price_original = clean_price(
                            str(variant.get("display_regular_price", ""))
                        )

                        # If no current price but original exists, use original
                        if not variant_price_current and variant_price_original:
                            variant_price_current = variant_price_original
                            variant_price_original = ""

                        variants.append(
                            {
                                "variant_id_native": str(variant_id),
                                "variant_title": variant_title,
                                "price_current": variant_price_current,
                                "price_original": variant_price_original,
                                "currency": "LKR",
                                "availability_text": availability,
                            }
                        )
                except (json.JSONDecodeError, TypeError):
                    # If variants data is invalid, fall back to default variant
                    pass

            # Default variant if no variants found
            if not variants:
                variants = [
                    {
                        "variant_id_native": product_id or str(int(time.time())),
                        "variant_title": "Default",
                        "price_current": price_current or "0",
                        "price_original": price_original,
                        "currency": "LKR",
                        "availability_text": availability,
                    }
                ]

            # Build final product data
            product_data = {
                "product_id_native": product_id or str(int(time.time())),
                "product_url": response.url,
                "product_title": product_title,
                "description_html": (
                    description_html.strip() if description_html else None
                ),
                "brand": brand if brand else None,
                "category_path": [cat.strip() for cat in category_path if cat.strip()],
                "specifications": specs,
                "image_urls": image_urls,
                "variants": variants,
                "metadata": {
                    "source_website": "lifemobile.lk",
                    "shop_contact_phone": "011 2322511",
                    "shop_contact_whatsapp": "077 7060616 / 077 55 77 115",
                    "scrape_timestamp": datetime.utcnow().isoformat() + "Z",
                    "script_id": "script2",
                },
            }

            self.products_collected += 1

            if self.products_collected % 10 == 0:
                elapsed = datetime.now() - self.start_time
                rate = self.products_collected / elapsed.total_seconds()
                logger.info(
                    f"Script2 Progress: {self.products_collected} products | {rate:.2f} products/sec | Errors: {self.error_count}"
                )

            yield product_data

        except Exception as e:
            self.error_count += 1
            logger.error(f"Error parsing product {response.url}: {str(e)}")

    def errback_handler(self, failure):
        self.error_count += 1
        logger.error(f"Request failed: {failure.request.url}")
        logger.error(f"Error: {failure.getErrorMessage()}")

    def closed(self, reason):
        elapsed = datetime.now() - self.start_time
        rate = (
            self.products_collected / elapsed.total_seconds()
            if elapsed.total_seconds() > 0
            else 0
        )
        logger.info(
            f"Script2 finished scraping. Total products: {self.products_collected}"
        )
        logger.info(f"Total time: {elapsed.total_seconds():.2f} seconds")
        logger.info(f"Scraping rate: {rate:.2f} products/sec")
        logger.info(f"Total errors: {self.error_count}")


if __name__ == "__main__":
    process = CrawlerProcess(
        settings={
            "FEED_FORMAT": "json",
            "FEED_URI": "lifemobile_products_script2.json",
            "LOG_LEVEL": "INFO",
            "CONCURRENT_REQUESTS": 4,
            "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
            "DOWNLOAD_DELAY": 0.5,
            "DOWNLOAD_TIMEOUT": 15,
            "AUTOTHROTTLE_ENABLED": True,
            "AUTOTHROTTLE_START_DELAY": 0.5,
            "ROBOTSTXT_OBEY": False,
            "COOKIES_ENABLED": False,
            "HTTPCACHE_ENABLED": False,
            "RETRY_ENABLED": True,
            "RETRY_TIMES": 2,
            "RETRY_HTTP_CODES": [500, 502, 503, 504, 429],
        }
    )
    process.crawl(LifeMobileSpider)
    process.start()

    # Create completion marker file to signal script is done
    import os

    with open("script2.complete", "w") as f:
        f.write("DONE")
    logger.info("✅ Script 2 complete marker created")
