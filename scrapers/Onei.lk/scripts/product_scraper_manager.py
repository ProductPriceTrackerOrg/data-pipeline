"""
Product scraping manager for Onei.lk
"""

from scrapy.crawler import CrawlerProcess
from scripts.product_scraper_core import One1LKSpider


def run_scraper():
    # Enhanced settings to ensure proper JSON output
    settings = {
        "FEED_FORMAT": "json",
        "FEED_URI": "one1lk_products.json",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_EXPORT_INDENT": 2,
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 32,  # Reduced to prevent overwhelming
        "CONCURRENT_REQUESTS_PER_DOMAIN": 16,
        "DOWNLOAD_TIMEOUT": 30,
        "DOWNLOAD_DELAY": 0.5,  # Increased delay to be more polite
        "RETRY_TIMES": 3,
        "FEEDS": {
            "one1lk_products.json": {
                "format": "json",
                "encoding": "utf-8",
                "indent": 2,
                "ensure_ascii": False,
            }
        },
    }

    process = CrawlerProcess(settings=settings)
    process.crawl(One1LKSpider)
    process.start()
