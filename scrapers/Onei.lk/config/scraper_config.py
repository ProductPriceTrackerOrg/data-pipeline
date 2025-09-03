"""
Configuration settings for Onei.lk scraper
"""

# Base configuration
BASE_URL = "https://onei.lk"
SHOP_URL = f"{BASE_URL}/shop/"

# Rotating user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

# Category extraction will be dynamic, but you can add static mappings if needed
CATEGORY_URLS = {
    # Example: "electronics": f"{BASE_URL}/shop/electronics/",
    # Add more if you want static category URLs
}

# Performance settings
MAX_CONCURRENT_REQUESTS = 64
REQUEST_DELAY = 0.1  # seconds between requests
TIMEOUT = 20  # request timeout in seconds
MAX_RETRIES = 2
RETRY_DELAY = 1  # seconds between retries

# Scrapy settings
SCRAPER_SETTINGS = {
    'FEED_FORMAT': 'json',
    'FEED_URI': 'one1lk_products.json',
    'LOG_LEVEL': 'INFO',
    'CONCURRENT_REQUESTS': MAX_CONCURRENT_REQUESTS,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 32,
    'DOWNLOAD_DELAY': REQUEST_DELAY,
    'RETRY_TIMES': MAX_RETRIES,
    'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408, 429],
    'DOWNLOAD_TIMEOUT': TIMEOUT,
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 0.5,
    'AUTOTHROTTLE_MAX_DELAY': 2,
    'HTTPCACHE_ENABLED': True,
    'HTTPCACHE_EXPIRATION_SECS': 3600,
    'HTTPCACHE_DIR': 'httpcache',
    'DNSCACHE_ENABLED': True,
    'FEED_EXPORT_ENCODING': 'utf-8',
}

# Request headers to mimic a real browser
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Output settings
OUTPUT_DIR = "scraped_data"
OUTPUT_FILE = "onei_products.json"
BACKUP_OUTPUT = True

# Shop metadata (hardcoded as requested)
SHOP_METADATA = {
    "source_website": "https://onei.lk",
    "shop_contact_phone": "+94770176666",
    "shop_contact_whatsapp": "+94770176666"
}

# Currency
DEFAULT_CURRENCY = "LKR"

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "scraper.log"
}