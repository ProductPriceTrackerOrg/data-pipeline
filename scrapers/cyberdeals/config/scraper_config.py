"""
Configuration settings for CyberDeals scraper
"""
from typing import Dict, List

# Base configuration
BASE_URL = "https://cyberdeals.lk"
SITEMAP_URL = f"{BASE_URL}/sitemap_index.xml"

# Performance settings
MAX_CONNECTIONS = 25  # Maximum concurrent connections
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
BATCH_SIZE = 100  # Number of products to process in each batch

# Request headers to mimic a real browser
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Output settings
OUTPUT_DIR = "scraped_data"
OUTPUT_FILE = "cyberdeals_lk_scrape_optimized.json"

# Shop metadata
SHOP_METADATA = {
    "source_website": "cyberdeals.lk",
    "shop_contact_phone": "+94 77 842 1245",
    "shop_contact_whatsapp": "+94 77 842 1245"
}

# Currency
DEFAULT_CURRENCY = "LKR"

# Known brands for more comprehensive brand extraction
KNOWN_BRANDS = [
    'apple', 'dahua', 'imou', 'hikvision', 'ezviz', 'ruijie', 'reyee', 
    'jovision', 'zotac', 'samsung', 'xiaomi', 'huawei', 'tp-link', 'ubiquiti',
    'dell', 'hp', 'lenovo', 'asus', 'acer', 'intel', 'amd', 'logitech', 'toshiba',
    'canon', 'nikon', 'sony', 'lg', 'microsoft', 'cisco', 'dlink', 'netgear',
    'seagate', 'western digital', 'wd', 'transcend', 'kingston', 'corsair'
]

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "cyberdeals_scraper.log"
}
