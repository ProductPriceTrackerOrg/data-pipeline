import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
import json
from datetime import datetime, timezone
import re
import random
from urllib.parse import urlparse
import logging
#djdjdd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class One1LKSpider(scrapy.Spider):
    name = 'one1lk'
    allowed_domains = ['onei.lk']
    start_urls = ['https://onei.lk/shop/']
    
    # Rotating user agents
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    ]
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 64,  # Increased concurrency
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'DOWNLOAD_DELAY': 0.1,  # Reduced delay
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408, 429],
        'DOWNLOAD_TIMEOUT': 20,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.5,
        'AUTOTHROTTLE_MAX_DELAY': 2,
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,
        'HTTPCACHE_DIR': 'httpcache',
        'DNSCACHE_ENABLED': True,
        'FEED_FORMAT': 'json',
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
    
    def __init__(self):
        self.products_collected = 0
        self.start_time = datetime.now(timezone.utc)
        self.category_urls = set()
        self.product_urls = set()
        self.error_count = 0
        self.rate_limit_delay = 1.0
        self.request_count = 0
    
    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, callback=self.parse, 
                         headers={'User-Agent': random.choice(self.USER_AGENTS)},
                         errback=self.errback_handler)
    
    def parse(self, response):
        # Extract all category URLs from the shop page
        category_links = response.css('ul.product-categories li a::attr(href)').getall()
        for url in category_links:
            if url not in self.category_urls:
                self.category_urls.add(url)
                yield Request(url=url, callback=self.parse_category, priority=1,
                            headers={'User-Agent': random.choice(self.USER_AGENTS)},
                            errback=self.errback_handler)
    
    def parse_category(self, response):
        # Skip if page failed to load
        if response.status in [404, 503, 429]:
            self.error_count += 1
            return
        
        # Extract product URLs from category page
        product_links = response.css('div.product-grid-item a.product-image-link::attr(href)').getall()
        for url in product_links:
            if url not in self.product_urls:
                self.product_urls.add(url)
                yield Request(url=url, callback=self.parse_product, priority=2,
                            headers={'User-Agent': random.choice(self.USER_AGENTS)},
                            errback=self.errback_handler)
        
        # Handle pagination if needed
        next_page = response.css('a.next::attr(href)').get()
        if next_page:
            yield Request(url=next_page, callback=self.parse_category, priority=1,
                        headers={'User-Agent': random.choice(self.USER_AGENTS)},
                        errback=self.errback_handler)
    
    def parse_product(self, response):
        # Skip if page failed to load
        if response.status in [404, 503, 429]:
            self.error_count += 1
            return
        
        # Optimized product ID extraction
        product_div = response.css('div[class*="single-product-page"]')
        product_id = product_div.attrib.get('id', '').replace('product-', '') or response.url.split('/')[-2]
        
        # Optimized title extraction
        product_title = response.css('h1.product_title::text').get(default='').strip()
        
        # Optimized description extraction
        description_html = (response.css('div.woocommerce-product-details__short-description').get() or
                          response.css('div[itemprop="description"]').get())
        
        # Optimized brand extraction
        brand = (response.css('span.posted_in a::text').get(default='').strip() or
                (response.css('nav.woocommerce-breadcrumb a::text').getall()[-1:][0].strip() 
                 if response.css('nav.woocommerce-breadcrumb a::text').getall() else ''))
        
        # Optimized category path
        category_path = (response.css('span.posted_in a::text').getall() or
                       response.css('nav.woocommerce-breadcrumb a::text').getall()[1:])
        
        # Optimized image extraction - single pass
        image_selectors = response.css('img.wp-post-image, img.attachment-woocommerce_thumbnail, div.wd-carousel-item a[href]')
        image_urls = list(set(
            img.attrib.get('src', img.attrib.get('href', ''))
            for img in image_selectors
            if img.attrib.get('src') or img.attrib.get('href')
        ))
        
        # Optimized variant processing
        variants = []
        is_variable = 'product-type-variable' in response.css('body').attrib.get('class', '')
        
        if is_variable:
            options = response.css('table.variations select option[value!=""]')
            price_current = response.css('p.price ins span.amount bdi::text, p.price span.amount bdi::text').get(default='').strip()
            price_original = response.css('p.price del span.amount bdi::text').get(default='').strip()
            availability = response.css('p.stock::text').get(default='In stock').strip()
            
            variants = [{
                "variant_id_native": opt.attrib.get('value'),
                "variant_title": opt.css('::text').get(default='').strip(),
                "price_current": price_current,
                "price_original": price_original if price_original else None,
                "currency": "Rs.",
                "availability_text": availability
            } for opt in options]
        else:
            price_current = response.css('p.price ins span.amount bdi::text, p.price span.amount bdi::text').get(default='').strip()
            price_original = response.css('p.price del span.amount bdi::text').get(default='').strip()
            availability = response.css('p.stock::text').get(default='In stock').strip()
            
            variants = [{
                "variant_id_native": product_id,
                "variant_title": "Default",
                "price_current": price_current,
                "price_original": price_original if price_original else None,
                "currency": "Rs.",
                "availability_text": availability
            }]
        
        # Construct final product data
        product_data = {
            "product_id_native": product_id,
            "product_url": response.url,
            "product_title": product_title,
            "description_html": description_html.strip() if description_html else None,
            "brand": brand if brand else None,
            "category_path": [cat.strip() for cat in category_path],
            "image_urls": image_urls,
            "variants": variants,
            "metadata": {
                "source_website": "https://onei.lk",
                "shop_contact_phone": "+94770176666",
                "shop_contact_whatsapp": "+94770176666",
                "scrape_timestamp": datetime.now(timezone.utc).isoformat()
            }
        }
        
        self.products_collected += 1
        self.request_count += 1
        
        if self.products_collected % 50 == 0:
            elapsed = datetime.now(timezone.utc) - self.start_time
            rate = self.products_collected / elapsed.total_seconds()
            logger.info(f"Progress: {self.products_collected} products | {rate:.2f} products/sec | Errors: {self.error_count}")
        
        yield product_data
    
    def errback_handler(self, failure):
        self.error_count += 1
        url = failure.request.url
        logger.error(f"Failed to process {url}: {failure.value}")
        
        # Handle rate limiting
        if failure.check(scrapy.exceptions.HttpError) and failure.value.response.status == 429:
            self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 10.0)
            logger.warning(f"Rate limited, increasing delay to {self.rate_limit_delay:.2f}s")
    
    def closed(self, reason):
        elapsed = datetime.now(timezone.utc) - self.start_time
        rate = self.products_collected / elapsed.total_seconds()
        success_rate = (self.products_collected / self.request_count) * 100 if self.request_count > 0 else 0
        
        logger.info(f"Finished scraping. Total products: {self.products_collected}")
        logger.info(f"Total time: {elapsed.total_seconds():.2f} seconds")
        logger.info(f"Scraping rate: {rate:.2f} products/sec")
        logger.info(f"Success rate: {success_rate:.2f}%")
        logger.info(f"Total errors: {self.error_count}")

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'json',
        'FEED_URI': 'one1lk_products.json',
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': 64,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
        'DOWNLOAD_TIMEOUT': 15,
    })
    process.crawl(One1LKSpider)
    process.start()
    ###