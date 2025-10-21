"""
Product scraping manager for orchestrating the scraping process
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import os

from scripts.product_scraper_core import ProductScraper
from models.product_models import ScrapingResult, Product
from utils.scraper_utils import (
    save_json_data, load_json_data, create_backup_filename,
    setup_logging, ensure_output_directory
)
from config.scraper_config import OUTPUT_DIR, OUTPUT_FILE, BACKUP_OUTPUT, LOGGING_CONFIG


class ScrapingManager:
    """High-level scraping manager"""
    
    def __init__(self, output_dir: str = OUTPUT_DIR, output_file: str = OUTPUT_FILE, save_locally: bool = False):
        self.output_dir = output_dir
        self.output_file = output_file
        self.output_path = os.path.join(output_dir, output_file)
        self.save_locally = save_locally  # Flag to control local file saving
        self.logger = logging.getLogger(__name__)
        
        # Ensure output directory exists if saving locally
        if self.save_locally:
            ensure_output_directory(output_dir)
    
    async def run_full_scraping(self) -> ScrapingResult:
        """
        Run complete scraping process
        
        Returns:
            ScrapingResult with all scraped data and metadata
        """
        self.logger.info("=== Starting SimplyTek Product Scraping ===")
        start_time = datetime.now(timezone.utc)
        
        try:
            # Create backup of existing data if it exists and we're saving locally
            if self.save_locally and BACKUP_OUTPUT and os.path.exists(self.output_path):
                await self._create_backup()
            
            # Initialize scraper and run scraping
            scraper = ProductScraper()
            result = await scraper.scrape_all_products()
            
            # Save results only if save_locally is True
            if self.save_locally:
                await self._save_results(result)
            
            # Log summary
            end_time = datetime.now(timezone.utc)
            duration = end_time - start_time
            
            self.logger.info("=== Scraping Completed Successfully ===")
            self.logger.info(f"Duration: {duration}")
            self.logger.info(f"Products scraped: {result.total_products}")
            self.logger.info(f"Variants scraped: {result.total_variants}")
            self.logger.info(f"Output saved to: {self.output_path}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            raise
    
    async def run_incremental_scraping(self) -> ScrapingResult:
        """
        Run incremental scraping (only new/updated products)
        
        Returns:
            ScrapingResult with new/updated products
        """
        self.logger.info("Starting incremental scraping")
        
        # Load existing data
        existing_data = await self._load_existing_data()
        existing_products = {}
        
        if existing_data:
            for product_data in existing_data.get('products', []):
                product_id = product_data.get('product_id_native')
                if product_id:
                    existing_products[product_id] = product_data
        
        # Run full scraping
        scraper = ProductScraper()
        full_result = await scraper.scrape_all_products()
        
        # Identify new/updated products
        new_products = []
        updated_products = []
        
        for product in full_result.products:
            product_id = product.product_id_native
            
            if product_id not in existing_products:
                new_products.append(product)
            else:
                # Check if product has been updated (simple comparison)
                existing_product = existing_products[product_id]
                if self._product_changed(product.dict(), existing_product):
                    updated_products.append(product)
        
        self.logger.info(f"Incremental results: {len(new_products)} new, {len(updated_products)} updated")
        
        # Create incremental result
        incremental_result = ScrapingResult(
            total_products=len(new_products) + len(updated_products),
            total_variants=sum(len(p.variants) for p in new_products + updated_products),
            categories_scraped=full_result.categories_scraped,
            scraping_metadata={
                **full_result.scraping_metadata,
                "incremental_mode": True,
                "new_products": len(new_products),
                "updated_products": len(updated_products)
            },
            products=new_products + updated_products
        )
        
        # Save incremental results
        incremental_file = f"incremental_{OUTPUT_FILE}"
        incremental_path = os.path.join(self.output_dir, incremental_file)
        await self._save_results(incremental_result, incremental_path)
        
        return incremental_result
    
    async def run_category_scraping(self, categories: List[str]) -> ScrapingResult:
        """
        Run scraping for specific categories only
        
        Args:
            categories: List of category names to scrape
            
        Returns:
            ScrapingResult with products from specified categories
        """
        self.logger.info(f"Starting category-specific scraping for: {categories}")
        
        scraper = ProductScraper()
        
        # Override category URLs to only include specified categories
        from config.scraper_config import CATEGORY_URLS
        original_categories = CATEGORY_URLS.copy()
        
        try:
            # Filter categories
            filtered_categories = {
                cat: url for cat, url in original_categories.items() 
                if cat in categories
            }
            
            if not filtered_categories:
                raise ValueError(f"None of the specified categories found: {categories}")
            
            # Temporarily update the config (for this session only)
            import config.scraper_config as scraper_config
            scraper_config.CATEGORY_URLS = filtered_categories
            
            # Run scraping
            result = await scraper.scrape_all_products()
            
            # Save results with category-specific filename
            category_str = "_".join(categories[:3])  # Limit filename length
            category_file = f"categories_{category_str}_{OUTPUT_FILE}"
            category_path = os.path.join(self.output_dir, category_file)
            await self._save_results(result, category_path)
            
            return result
            
        finally:
            # Restore original categories
            import scrapers.simplytek.config.scraper_config as scraper_config
            scraper_config.CATEGORY_URLS = original_categories
    
    async def _create_backup(self) -> None:
        """Create backup of existing data"""
        try:
            backup_file = create_backup_filename(self.output_file)
            backup_path = os.path.join(self.output_dir, backup_file)
            
            # Load existing data and save as backup
            existing_data = load_json_data(self.output_path)
            if existing_data:
                save_json_data(existing_data, backup_path)
                self.logger.info(f"Backup created: {backup_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to create backup: {e}")
    
    async def _save_results(self, result: ScrapingResult, custom_path: Optional[str] = None) -> None:
        """Save scraping results to file (products only)"""
        # Skip if save_locally is False (unless this is a custom path request)
        if not self.save_locally and custom_path is None:
            self.logger.info("Local file saving is disabled, skipping file write")
            return
            
        output_path = custom_path or self.output_path
        
        try:
            # Extract only the products array from the result
            products_data = [product.dict() for product in result.products]
            
            # Save to file
            success = save_json_data(products_data, output_path)
            
            if success:
                self.logger.info(f"Results saved successfully to: {output_path}")
            else:
                raise Exception("Failed to save results")
                
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            raise
    
    async def _load_existing_data(self) -> Optional[Dict[str, Any]]:
        """Load existing scraped data"""
        if os.path.exists(self.output_path):
            return load_json_data(self.output_path)
        return None
    
    def _product_changed(self, new_product: Dict[str, Any], old_product: Dict[str, Any]) -> bool:
        """
        Simple comparison to detect if product has changed
        
        Args:
            new_product: New product data
            old_product: Existing product data
            
        Returns:
            True if product has changed, False otherwise
        """
        # Compare key fields that commonly change
        fields_to_compare = [
            'product_title', 'description_html', 'variants', 'image_urls'
        ]
        
        for field in fields_to_compare:
            if new_product.get(field) != old_product.get(field):
                return True
        
        return False
    
    async def validate_scraped_data(self, data_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Validate scraped data against schema
        
        Args:
            data_path: Path to data file (uses default if None)
            
        Returns:
            Validation report
        """
        file_path = data_path or self.output_path
        
        if not os.path.exists(file_path):
            return {"valid": False, "error": "File does not exist"}
        
        try:
            # Load data
            data = load_json_data(file_path)
            if not data:
                return {"valid": False, "error": "Failed to load data"}
            
            # Validate against ScrapingResult model
            result = ScrapingResult(**data)
            
            # Additional validation checks
            validation_report = {
                "valid": True,
                "total_products": len(result.products),
                "total_variants": sum(len(p.variants) for p in result.products),
                "validation_checks": {
                    "all_products_have_variants": all(len(p.variants) > 0 for p in result.products),
                    "all_products_have_valid_ids": all(p.product_id_native for p in result.products),
                    "all_variants_have_prices": all(
                        v.price_current for p in result.products for v in p.variants
                    ),
                    "all_products_have_urls": all(p.product_url for p in result.products)
                }
            }
            
            # Count any failed checks
            failed_checks = sum(1 for check, passed in validation_report["validation_checks"].items() if not passed)
            validation_report["warnings"] = failed_checks
            
            return validation_report
            
        except Exception as e:
            return {"valid": False, "error": f"Validation error: {e}"}
    
    async def get_scraping_stats(self) -> Dict[str, Any]:
        """Get statistics about scraped data"""
        if not os.path.exists(self.output_path):
            return {"error": "No scraped data found"}
        
        try:
            data = load_json_data(self.output_path)
            if not data:
                return {"error": "Failed to load data"}
            
            result = ScrapingResult(**data)
            
            # Calculate statistics
            brands = set()
            categories = set()
            total_images = 0
            price_ranges = {"min": float('inf'), "max": 0}
            availability_counts = {"in_stock": 0, "out_of_stock": 0}
            
            for product in result.products:
                if product.brand:
                    brands.add(product.brand)
                
                if product.category_path:
                    categories.add(tuple(product.category_path))
                
                total_images += len(product.image_urls)
                
                for variant in product.variants:
                    try:
                        price = float(variant.price_current)
                        price_ranges["min"] = min(price_ranges["min"], price)
                        price_ranges["max"] = max(price_ranges["max"], price)
                    except ValueError:
                        pass
                    
                    if "in stock" in variant.availability_text.lower():
                        availability_counts["in_stock"] += 1
                    else:
                        availability_counts["out_of_stock"] += 1
            
            if price_ranges["min"] == float('inf'):
                price_ranges["min"] = 0
            
            return {
                "total_products": result.total_products,
                "total_variants": result.total_variants,
                "unique_brands": len(brands),
                "unique_categories": len(categories),
                "total_images": total_images,
                "price_range": price_ranges,
                "availability": availability_counts,
                "scraping_metadata": result.scraping_metadata
            }
            
        except Exception as e:
            return {"error": f"Error calculating stats: {e}"}


def setup_scraping_environment():
    """Setup the scraping environment (logging, directories, etc.)"""
    # Setup logging
    setup_logging(
        log_file=LOGGING_CONFIG.get("file", "scraper.log"),
        level=LOGGING_CONFIG.get("level", "INFO")
    )
    
    # Create output directory
    ensure_output_directory(OUTPUT_DIR)
    logging.getLogger(__name__).info("Using output directory: %s", OUTPUT_DIR)
    
    # Check for Brotli support - website uses brotli compression
    logger = logging.getLogger(__name__)
    try:
        import brotli
        logger.info("Brotli compression support detected")
    except ImportError:
        logger.warning("Brotli compression support not found. Install with 'pip install brotli'")
        logger.warning("This may cause failures when scraping compressed responses")
    
    logging.info("Scraping environment initialized")


if __name__ == "__main__":
    # Basic test run
    async def test_run():
        setup_scraping_environment()
        manager = ScrapingManager()
        result = await manager.run_full_scraping()
        print(f"Scraping completed: {result.total_products} products")
    
    asyncio.run(test_run())