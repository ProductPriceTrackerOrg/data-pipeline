"""
Main scraper application that orchestrates the entire scraping process
"""
import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any

from models.product_models import ScrapingResultModel
from config.scraper_config import ScraperConfig
from utils.scraper_utils import AsyncRequestManager, ScraperUtils
from scripts.category_scraper import CategoryScraper
from scripts.product_scraper import ProductBatchScraper


class AppleMeScraper:
    """Main scraper class that orchestrates the entire scraping process"""
    
    def __init__(self):
        self.logger = ScraperUtils.setup_logging()
        self.utils = ScraperUtils()
        self.category_scraper = CategoryScraper()
        self.product_scraper = ProductBatchScraper()
        
        # Create output directory
        os.makedirs(ScraperConfig.OUTPUT_DIR, exist_ok=True)
    
    async def run_full_scrape(self) -> ScrapingResultModel:
        """Run the complete scraping process"""
        start_time = datetime.now()
        self.logger.info("Starting AppleMe.lk scraping process...")
        
        async with AsyncRequestManager() as request_manager:
            # Step 1: Scrape categories and get product lists
            self.logger.info("Step 1: Scraping categories...")
            category_products = await self.category_scraper.scrape_all_categories(request_manager)
            
            if not category_products:
                self.logger.error("No categories found. Exiting.")
                return self._create_empty_result(start_time)
            
            # Step 2: Flatten product list
            all_products_info = []
            categories_processed = len(category_products)
            
            for category_name, products in category_products.items():
                self.logger.info(f"Found {len(products)} products in {category_name}")
                all_products_info.extend(products)
            
            self.logger.info(f"Total products to scrape: {len(all_products_info)}")
            
            if not all_products_info:
                self.logger.warning("No products found in any category")
                return self._create_empty_result(start_time)
            
            # Step 3: Scrape detailed product information
            self.logger.info("Step 2: Scraping detailed product information...")
            successful_products = await self.product_scraper.scrape_products_batch(
                request_manager, all_products_info
            )
            
            # Step 4: Save results
            end_time = datetime.now()
            
            result = ScrapingResultModel(
                total_products=len(all_products_info),
                successful_scrapes=len(successful_products),
                failed_scrapes=len(all_products_info) - len(successful_products),
                categories_processed=categories_processed,
                start_time=start_time,
                end_time=end_time,
                products=successful_products
            )
            
            # Save data
            await self._save_results(result, category_products, all_products_info)
            
            return result
    
    async def run_category_scrape(self, category_names: List[str]) -> ScrapingResultModel:
        """Run scraping for specific categories only"""
        start_time = datetime.now()
        self.logger.info(f"Starting targeted scraping for categories: {category_names}")
        
        async with AsyncRequestManager() as request_manager:
            # Get all categories first
            all_categories = await self.category_scraper.get_main_categories(request_manager)
            
            # Filter categories
            target_categories = [
                cat for cat in all_categories 
                if cat['name'].lower() in [name.lower() for name in category_names]
            ]
            
            if not target_categories:
                self.logger.error("No matching categories found")
                return self._create_empty_result(start_time)
            
            # Scrape target categories
            category_products = {}
            for category in target_categories:
                products = await self.category_scraper.get_category_products(
                    request_manager, category['url'], category['name']
                )
                category_products[category['name']] = products
            
            # Process products
            all_products_info = []
            for products in category_products.values():
                all_products_info.extend(products)
            
            self.logger.info(f"Total products to scrape: {len(all_products_info)}")
            
            # Scrape detailed information
            successful_products = await self.product_scraper.scrape_products_batch(
                request_manager, all_products_info
            )
            
            # Create result
            end_time = datetime.now()
            result = ScrapingResultModel(
                total_products=len(all_products_info),
                successful_scrapes=len(successful_products),
                failed_scrapes=len(all_products_info) - len(successful_products),
                categories_processed=len(target_categories),
                start_time=start_time,
                end_time=end_time,
                products=successful_products
            )
            
            # Save results
            await self._save_results(result, category_products, all_products_info)
            
            return result
    
    async def _save_results(self, result: ScrapingResultModel, 
                          category_products: Dict, all_products_info: List):
        """Save scraping results to files"""
        self.logger.info("Saving results...")
        
        # Save main products file
        products_file = os.path.join(ScraperConfig.OUTPUT_DIR, ScraperConfig.PRODUCTS_FILE)
        products_data = {
            'scrape_info': {
                'total_products': result.total_products,
                'successful_scrapes': result.successful_scrapes,
                'failed_scrapes': result.failed_scrapes,
                'categories_processed': result.categories_processed,
                'start_time': result.start_time.isoformat(),
                'end_time': result.end_time.isoformat(),
                'duration': str(result.end_time - result.start_time)
            },
            'products': [product.dict() for product in result.products]
        }
        
        if self.utils.save_json(products_data, products_file):
            self.logger.info(f"Products saved to {products_file}")
        
        # Save failed products for retry
        failed_products = [
            product for product in all_products_info 
            if not any(p.product_url == product['url'] for p in result.products)
        ]
        
        if failed_products:
            failed_file = os.path.join(ScraperConfig.OUTPUT_DIR, ScraperConfig.FAILED_PRODUCTS_FILE)
            failed_data = {
                'failed_count': len(failed_products),
                'products': failed_products,
                'scrape_timestamp': datetime.now().isoformat()
            }
            
            if self.utils.save_json(failed_data, failed_file):
                self.logger.info(f"Failed products saved to {failed_file}")
    
    def _create_empty_result(self, start_time: datetime) -> ScrapingResultModel:
        """Create empty result for failed scraping"""
        return ScrapingResultModel(
            total_products=0,
            successful_scrapes=0,
            failed_scrapes=0,
            categories_processed=0,
            start_time=start_time,
            end_time=datetime.now(),
            products=[]
        )
    
    def print_summary(self, result: ScrapingResultModel):
        """Print scraping summary"""
        duration = result.end_time - result.start_time
        
        print("\n" + "="*60)
        print("APPLEME.LK SCRAPING SUMMARY")
        print("="*60)
        print(f"Start Time: {result.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End Time: {result.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {duration}")
        print(f"Categories Processed: {result.categories_processed}")
        print(f"Total Products Found: {result.total_products}")
        print(f"Successfully Scraped: {result.successful_scrapes}")
        print(f"Failed: {result.failed_scrapes}")
        
        if result.total_products > 0:
            success_rate = (result.successful_scrapes / result.total_products) * 100
            print(f"Success Rate: {success_rate:.2f}%")
        
        print("="*60)


async def main():
    """Main entry point"""
    scraper = AppleMeScraper()
    
    try:
        # Run full scraping process
        result = await scraper.run_full_scrape()
        
        # Print summary
        scraper.print_summary(result)
        
        return result
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        scraper.logger.error(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())