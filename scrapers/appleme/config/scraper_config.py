"""
Central configuration module for AppleMe.lk scraper

This module contains all configuration constants and settings used throughout
the scraping application. It provides optimized settings for performance
while maintaining reliability and avoiding rate limits.
"""
import random
from typing import Dict, List


class ScraperConfig:
    """
    Central configuration class containing all scraper settings
    
    This class provides optimized configuration for high-performance scraping
    of AppleMe.lk while avoiding rate limits and maintaining success rates.
    All settings are tuned for speed without compromising data quality.
    """
    
    # Base URLs and endpoints
    BASE_URL = "https://appleme.lk"
    CATEGORIES_URL = "https://appleme.lk/shop/"  # Main shop page with navigation menu
    
    # HTTP request configuration - optimized for speed
    REQUEST_TIMEOUT = 15  # Timeout per request in seconds (reduced for faster processing)
    MAX_RETRIES = 1  # Maximum retry attempts per failed request (reduced for speed)
    RETRY_DELAY = 0.5  # Delay between retries in seconds
    
    # Rate limiting configuration - aggressive but safe settings
    MIN_DELAY = 0.1  # Minimum delay between requests (very aggressive for speed)
    MAX_DELAY = 0.5  # Maximum delay between requests (keeps requests fast)
    RATE_LIMIT_DELAY = 15  # Delay when encountering 429 rate limits
    
    # Concurrency settings - optimized for maximum throughput
    MAX_CONCURRENT_REQUESTS = 15  # Concurrent product detail requests (increased from 8)
    MAX_CONCURRENT_CATEGORIES = 8  # Concurrent category processing (increased from 5)
    BATCH_SIZE = 100  # Products processed per batch (larger batches for efficiency)
    
    # Shop metadata and contact information
    SHOP_CONTACT_PHONE = "+94 77 791 1011"
    SHOP_CONTACT_WHATSAPP = "+94 77 791 1011"
    SOURCE_WEBSITE = "appleme.lk"
    
    # User agent rotation for request diversity
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ]
    
    # File output configuration
    OUTPUT_DIR = "scraped_data"  # Directory for storing scraped data
    PRODUCTS_FILE = "appleme_products.json"  # Main products data file
    FAILED_PRODUCTS_FILE = "failed_products.json"  # Failed scraping attempts log
    
    @classmethod
    def get_random_user_agent(cls) -> str:
        """Get a random user agent"""
        return random.choice(cls.USER_AGENTS)
    
    @classmethod
    def get_random_delay(cls) -> float:
        """Get a random delay between min and max delay"""
        return random.uniform(cls.MIN_DELAY, cls.MAX_DELAY)
    
    @classmethod
    def get_headers(cls) -> Dict[str, str]:
        """Get request headers"""
        return {
            'User-Agent': cls.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }


class SelectorConfig:
    """CSS selectors for scraping"""
    
    # Category page selectors
    CATEGORY_LINKS = "ul#menu-leftmenu li.menu-item-object-product_cat > a"
    CATEGORY_COUNT = "ul#menu-leftmenu li.menu-item-object-product_cat .count"
    
    # Product listing selectors
    PRODUCT_ITEMS = "li.product"
    PRODUCT_LINK = "a.woocommerce-LoopProduct-link"
    PRODUCT_TITLE = "h2.woocommerce-loop-product__title"
    PRODUCT_IMAGE = "div.product-thumbnail img"
    PRODUCT_PRICE = "span.price .woocommerce-Price-amount"
    PRODUCT_CATEGORY = "span.loop-product-categories a"
    STOCK_STATUS = "p.stock"
    
    # Pagination
    NEXT_PAGE = "a.next.page-numbers"
    PAGE_NUMBERS = "a.page-numbers"
    
    # Product detail page selectors
    BREADCRUMB = "nav.woocommerce-breadcrumb a"
    PRODUCT_TITLE_DETAIL = "h1.product_title"
    PRODUCT_DESCRIPTION = "div.woocommerce-product-details__short-description"
    PRODUCT_IMAGES = "div.woocommerce-product-gallery img"
    PRODUCT_BRAND = "div.pwb-single-product-brands img"
    PRICE_CURRENT = "p.price .woocommerce-Price-amount"
    PRICE_CASH = "div.payment-options div.cash strong"
    AVAILABILITY = "div.availability p.stock"
    PRODUCT_ID = "button[name='add-to-cart']"