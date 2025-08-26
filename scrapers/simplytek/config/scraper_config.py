"""
Configuration settings for SimplyTek scraper
"""
import os
from typing import Dict, List

# Base configuration
BASE_URL = "https://www.simplytek.lk"
API_BASE_URL = f"{BASE_URL}/collections"

# Category URLs mapping
CATEGORY_URLS = {
    "apple-store": f"{API_BASE_URL}/apple-store-sri-lanka/products.json",
    "mobile-phones": f"{API_BASE_URL}/mobile-phones/products.json",
    "smartwatches": f"{API_BASE_URL}/smartwatches/products.json",
    "earphones-and-headphones": f"{API_BASE_URL}/earphones-and-headphones/products.json",
    "power-banks": f"{API_BASE_URL}/power-banks/products.json",
    "mobile-phone-accessories": f"{API_BASE_URL}/mobile-phone-accessories/products.json",
    "smart-devices": f"{API_BASE_URL}/smart-devices/products.json",
    "speakers": f"{API_BASE_URL}/speakers/products.json",
    "cameras": f"{API_BASE_URL}/cameras/products.json",
    "projectors": f"{API_BASE_URL}/projectors/products.json",
    "storage-devices": f"{API_BASE_URL}/storage-devices/products.json",
    "computer-accessories": f"{API_BASE_URL}/computer-accessories/products.json",
    "car-accessories": f"{API_BASE_URL}/car-accessories/products.json",
    "cases-and-backcover": f"{API_BASE_URL}/cases-and-backcover/products.json",
    "tools": f"{API_BASE_URL}/tools/products.json",
    "personal-care": f"{API_BASE_URL}/personal-care/products.json",
    "home-care": f"{API_BASE_URL}/home-care/products.json",
}

# Alternative: Use the all products endpoint for comprehensive scraping
ALL_PRODUCTS_URL = f"{API_BASE_URL}/all/products.json"

# Performance settings
MAX_CONCURRENT_REQUESTS = 10
REQUEST_DELAY = 0.1  # seconds between requests
TIMEOUT = 30  # request timeout in seconds
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds between retries

# Request headers to mimic a real browser
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",  # Removed 'br' to avoid Brotli issues
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# Output settings
OUTPUT_DIR = "scraped_data"
OUTPUT_FILE = "simplytek_products.json"
BACKUP_OUTPUT = True

# Shop metadata (hardcoded as requested)
SHOP_METADATA = {
    "source_website": "www.simplytek.lk",
    "shop_contact_phone": "+94 117 555 888",
    "shop_contact_whatsapp": "+94 72 672 9729"
}

# Currency
DEFAULT_CURRENCY = "LKR"

# Category path mapping for better organization
CATEGORY_PATH_MAPPING = {
    "apple-store-sri-lanka": ["Electronics", "Apple Store"],
    "mobile-phones": ["Electronics", "Mobile Phones"],
    "smartwatches": ["Electronics", "Wearables", "Smartwatches"],
    "earphones-and-headphones": ["Electronics", "Audio", "Earphones & Headphones"],
    "power-banks": ["Electronics", "Accessories", "Power Banks"],
    "mobile-phone-accessories": ["Electronics", "Accessories", "Mobile Phone Accessories"],
    "smart-devices": ["Electronics", "Smart Devices"],
    "speakers": ["Electronics", "Audio", "Speakers"],
    "cameras": ["Electronics", "Photography", "Cameras"],
    "projectors": ["Electronics", "Display", "Projectors"],
    "storage-devices": ["Electronics", "Storage", "Storage Devices"],
    "computer-accessories": ["Electronics", "Computers", "Accessories"],
    "car-accessories": ["Automotive", "Car Accessories"],
    "cases-and-backcover": ["Electronics", "Accessories", "Cases & Back Covers"],
    "tools": ["Tools & Hardware"],
    "personal-care": ["Personal Care"],
    "home-care": ["Home & Garden", "Home Care"],
}

# Logging configuration
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "scraper.log"
}