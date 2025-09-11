"""
Scraping manager for LifeMobile
"""
import asyncio
import logging
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from scripts.product_scraper_core import LifeMobileSpider
from models.product_models import Product, ScrapingResult, LifeMobileScrapeStats
from config.scraper_config import LifeMobileConfig
from utils.scraper_utils import setup_logging, clean_json_data, validate_product_data

logger = logging.getLogger(__name__)


class LifeMobileScrapingManager:
    """Manager for coordinating LifeMobile scraping operations"""
    
    def __init__(self, output_file: str = "lifemobile_products.json", save_locally: bool = True):
        self.output_file = output_file
        self.save_locally = save_locally
        self.config = LifeMobileConfig()
        self.stats = LifeMobileScrapeStats(start_time=datetime.now(timezone.utc))
        
    def run_scraper(self) -> bool:
        """Run the Scrapy spider"""
        try:
            # Configure Scrapy settings
            settings = self.config.get_scrapy_settings()
            
            if self.save_locally:
                settings.update({
                    'FEED_FORMAT': 'json',
                    'FEED_URI': self.output_file,
                    'FEED_EXPORT_ENCODING': 'utf-8',
                    'FEED_EXPORT_INDENT': 2,
                })
            
            # Create and run crawler process
            process = CrawlerProcess(settings)
            process.crawl(LifeMobileSpider)
            process.start()
            
            logger.info(f"Scraping completed. Output: {self.output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error running scraper: {e}")
            return False
    
    async def run_full_scraping(self) -> ScrapingResult:
        """Run complete scraping process and return structured results"""
        try:
            logger.info("Starting LifeMobile scraping process...")
            
            # Run the scraper
            success = self.run_scraper()
            if not success:
                raise Exception("Scraping failed")
            
            # Load and process results
            products = []
            total_variants = 0
            
            if self.save_locally and os.path.exists(self.output_file):
                products = await self._load_products_from_file()
                total_variants = sum(len(p.variants) for p in products)
                
                # Update stats
                self.stats.products_scraped = len(products)
                self.stats.finish_scraping()
            
            # Create scraping result
            result = ScrapingResult(
                total_products=len(products),
                total_variants=total_variants,
                total_errors=self.stats.errors_encountered,
                scraping_metadata={
                    "start_time": self.stats.start_time.isoformat(),
                    "end_time": self.stats.end_time.isoformat() if self.stats.end_time else None,
                    "duration_seconds": self.stats.duration_seconds,
                    "scraping_rate": self.stats.scraping_rate,
                    "source_website": "lifemobile.lk"
                },
                products=products
            )
            
            logger.info(f"Scraping completed: {len(products)} products, {total_variants} variants")
            return result
            
        except Exception as e:
            logger.error(f"Error in full scraping process: {e}")
            raise
    
    async def _load_products_from_file(self) -> List[Product]:
        """Load and validate products from JSON file"""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
            
            # Clean and validate data
            cleaned_data = clean_json_data(raw_data)
            
            products = []
            for item in cleaned_data:
                try:
                    if validate_product_data(item):
                        product = Product(**item)
                        products.append(product)
                except Exception as e:
                    logger.warning(f"Invalid product data: {e}")
                    self.stats.errors_encountered += 1
            
            logger.info(f"Loaded {len(products)} valid products from {self.output_file}")
            return products
            
        except Exception as e:
            logger.error(f"Error loading products from file: {e}")
            return []
    
    def cleanup_files(self):
        """Clean up temporary files"""
        try:
            if os.path.exists(self.output_file):
                os.remove(self.output_file)
                logger.info(f"Cleaned up file: {self.output_file}")
        except Exception as e:
            logger.warning(f"Could not clean up file {self.output_file}: {e}")


def setup_scraping_environment():
    """Set up the scraping environment"""
    # Create necessary directories
    directories = ["logs", "data", "temp"]
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    # Setup logging
    setup_logging("lifemobile_scraper.log")
    
    logger.info("LifeMobile scraping environment setup completed")


def run_lifemobile_scraper():
    """Simple function to run the scraper"""
    try:
        # Setup environment
        setup_scraping_environment()
        
        # Create and run scraper
        manager = LifeMobileScrapingManager()
        success = manager.run_scraper()
        
        if success:
            logger.info("LifeMobile scraping completed successfully")
        else:
            logger.error("LifeMobile scraping failed")
            
        return success
        
    except Exception as e:
        logger.error(f"Error in run_lifemobile_scraper: {e}")
        return False


# Async version for compatibility with main.py
async def run_lifemobile_scraper_async() -> ScrapingResult:
    """Async wrapper for scraping"""
    setup_scraping_environment()
    manager = LifeMobileScrapingManager(save_locally=False)  # Don't save locally for direct upload
    return await manager.run_full_scraping()


if __name__ == "__main__":
    run_lifemobile_scraper()