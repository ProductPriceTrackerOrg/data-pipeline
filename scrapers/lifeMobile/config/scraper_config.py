"""
LifeMobile scraper configuration settings
"""
from typing import Dict, Any, List

class LifeMobileConfig:
    """Configuration class for LifeMobile scraper"""
    
    # Basic spider settings
    SPIDER_NAME = "lifemobile"
    ALLOWED_DOMAINS = ["lifemobile.lk"]
    START_URLS = ["https://lifemobile.lk/"]
    
    # Performance settings
    CONCURRENT_REQUESTS = 100
    CONCURRENT_REQUESTS_PER_DOMAIN = 20
    DOWNLOAD_DELAY = 0.05
    DOWNLOAD_TIMEOUT = 15
    RETRY_TIMES = 1
    RETRY_HTTP_CODES = [500, 502, 503, 504, 429]
    
    # Limits
    MAX_PRODUCTS = 60000
    MAX_RUNTIME = 7200  # 2 hours
    MAX_PAGINATION_DEPTH = 15
    
    # User agents for rotation
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    ]
    
    # CSS Selectors
    SELECTORS = {
        'category_links': [
            "ul#menu-canvas-menu li.yamm-tfw a.dropdown-toggle::attr(href)",
            "ul#menu-canvas-menu li.menu-item a::attr(href)",
            "nav.woocommerce-breadcrumb a::attr(href)",
            "div.widget_product_categories a::attr(href)",
        ],
        'subcategory_links': [
            "ul.product-categories li a::attr(href)",
            ".widget_product_categories a::attr(href)",
            "div.product-category a::attr(href)",
        ],
        'product_links': [
            "a.woocommerce-LoopProduct-link::attr(href)",
            "h2.woocommerce-loop-product__title a::attr(href)",
            "li.product a::attr(href)",
        ],
        'pagination': [
            "a.next::attr(href)",
            "a.next.page-numbers::attr(href)",
            "a.page-numbers:not(.prev)::attr(href)",
        ]
    }
    
    # Product field selectors
    PRODUCT_SELECTORS = {
        'product_id': "div.product::attr(id)",
        'title': "h1.product_title::text",
        'description': "div.woocommerce-product-details__short-description",
        'brand_img': "div.brand img::attr(alt)",
        'brand_text': "span.posted_in a::text",
        'category_path': "span.posted_in a::text, nav.woocommerce-breadcrumb a::text",
        'images': "div.woocommerce-product-gallery img::attr(src), img.wp-post-image::attr(src)",
        'availability': "p.stock::text",
        'price_current': "p.price ins span.amount bdi::text, p.price span.amount bdi::text",
        'price_original': "p.price del span.amount bdi::text",
        'discount': "span.percentage::text",
        'variants_data': "form.variations_form::attr(data-product_variations)",
        'specs': "div.woocommerce-product-details__short-description li, div.aps-feature-info div",
        'payment_options': "div.mayapgs-payment"
    }
    
    # Scrapy settings
    SCRAPY_SETTINGS = {
        "CONCURRENT_REQUESTS": CONCURRENT_REQUESTS,
        "CONCURRENT_REQUESTS_PER_DOMAIN": CONCURRENT_REQUESTS_PER_DOMAIN,
        "DOWNLOAD_DELAY": DOWNLOAD_DELAY,
        "RETRY_TIMES": RETRY_TIMES,
        "RETRY_HTTP_CODES": RETRY_HTTP_CODES,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "AUTOTHROTTLE_ENABLED": False,
        "HTTPCACHE_ENABLED": False,
        "DNSCACHE_ENABLED": True,
        "FEED_FORMAT": "json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "USER_AGENT": USER_AGENTS[0],
        "ROBOTSTXT_OBEY": False,
        "COOKIES_ENABLED": False,
        "DOWNLOAD_MAXSIZE": 0,
        "REACTOR_THREADPOOL_MAXSIZE": 30,
        "SCHEDULER_PRIORITY_QUEUE": "scrapy.pqueues.DownloaderAwarePriorityQueue",
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
    }
    
    # Contact information
    SHOP_CONTACT = {
        "phone": "+9411 2322511",
        "whatsapp": "+9477 7060616",
        "website": "lifemobile.lk"
    }
    
    @classmethod
    def get_scrapy_settings(cls) -> Dict[str, Any]:
        """Get Scrapy settings dictionary"""
        return cls.SCRAPY_SETTINGS.copy()
    
    @classmethod
    def get_selectors(cls) -> Dict[str, List[str]]:
        """Get CSS selectors dictionary"""
        return cls.SELECTORS.copy()
    
    @classmethod
    def get_product_selectors(cls) -> Dict[str, str]:
        """Get product field selectors"""
        return cls.PRODUCT_SELECTORS.copy()