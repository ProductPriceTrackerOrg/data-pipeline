"""
LifeMobile Scraper Configuration
Optimized settings for speed and image URL validation
"""

# Speed Optimization Settings
SPEED_OPTIMIZED_SETTINGS = {
    "CONCURRENT_REQUESTS": 8,  # Increased from 4 for speed
    "CONCURRENT_REQUESTS_PER_DOMAIN": 4,  # Increased from 2 for speed
    "DOWNLOAD_DELAY": 0.25,  # Reduced from 0.5 for speed
    "RETRY_TIMES": 1,  # Reduced from 2 for speed
    "DOWNLOAD_TIMEOUT": 10,  # Reduced from 15 for speed
    "AUTOTHROTTLE_START_DELAY": 0.25,  # Reduced from 0.5 for speed
    "AUTOTHROTTLE_MAX_DELAY": 3,  # Reduced from 5 for speed
}

# Image URL Validation Settings
VALID_IMAGE_EXTENSIONS = (".webp", ".jpg", ".jpeg", ".png")

INVALID_IMAGE_PATTERNS = [
    "data:image/svg+xml",  # SVG placeholders
    "woocommerce-placeholder.png",  # WooCommerce placeholders
    "data:",  # Data URLs
]

# Performance Monitoring
PERFORMANCE_TARGETS = {
    "products_per_minute": 50,  # Target scraping speed
    "max_runtime_minutes": 30,  # Maximum runtime per script
    "success_rate_percent": 85,  # Target success rate
    "image_validity_percent": 95,  # Target valid image URLs
}

# Quality Thresholds
QUALITY_THRESHOLDS = {
    "min_image_urls_per_product": 1,
    "max_image_urls_per_product": 10,
    "required_fields": [
        "product_id_native",
        "product_url",
        "product_title",
        "image_urls",
    ],
}


def get_optimized_settings():
    """Return optimized Scrapy settings"""
    return {
        **SPEED_OPTIMIZED_SETTINGS,
        "HTTPCACHE_ENABLED": False,
        "DNSCACHE_ENABLED": True,
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": False,
        "REACTOR_THREADPOOL_MAXSIZE": 20,
    }
