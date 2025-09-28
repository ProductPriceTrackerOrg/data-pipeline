"""
Product scraping manager for orchestrating the Nanotek scraping process
"""

import asyncio
import logging
import json
import os
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from scripts.product_scraper_core import NanotekScraperCore
from models.product_models import ScrapingResult, Product, ScrapingStats
from utils.scraper_utils import (
    save_json_data,
    load_json_data,
    create_backup_filename,
    setup_logging,
    ensure_output_directory,
    upload_to_adls,
)
from config.scraper_config import OUTPUT_DIR, OUTPUT_FILE, BACKUP_OUTPUT, LOGGING_CONFIG


class NanotekScrapingManager:
    """High-level scraping manager for Nanotek"""

    def __init__(
        self,
        output_dir: str = OUTPUT_DIR,
        output_file: str = OUTPUT_FILE,
        enable_azure: bool = True,
        save_locally: bool = True,
    ):
        self.output_dir = output_dir
        self.output_file = output_file
        self.output_path = os.path.join(output_dir, output_file)
        self.enable_azure = enable_azure
        self.save_locally = save_locally
        self.logger = logging.getLogger(__name__)

        # Ensure output directory exists if saving locally
        if self.save_locally:
            ensure_output_directory(output_dir)

        # Initialize core scraper
        self.scraper = NanotekScraperCore()

    async def run_full_scraping(self, resume: bool = False) -> ScrapingResult:
        """
        Run complete scraping process

        Args:
            resume: Whether to resume from previous session

        Returns:
            ScrapingResult with all scraped data and metadata
        """
        self.logger.info("=== Starting Nanotek Product Scraping ===")
        start_time = datetime.now()

        try:
            # Clean existing data if not resuming
            if not resume and self.save_locally:
                self._clean_existing_data()

            # Get product URLs
            url_file = "all_product_urls.json"
            if resume and os.path.exists(url_file):
                with open(url_file, "r", encoding="utf-8") as f:
                    all_product_urls = json.load(f)
                self.logger.info(
                    f"🔄 Resuming with {len(all_product_urls)} URLs from previous session"
                )
            else:
                all_product_urls = self.scraper.get_all_product_urls()

                # Save URLs for potential resuming
                if self.save_locally:
                    with open(url_file, "w", encoding="utf-8") as f:
                        json.dump(all_product_urls, f, indent=2, ensure_ascii=False)

            if not all_product_urls:
                self.logger.error("❌ No product URLs found!")
                return ScrapingResult(
                    total_products=0,
                    total_variants=0,
                    categories_scraped=[],
                    scraping_metadata={"error": "No product URLs found"},
                    products=[],
                )

            # Process in batches
            from config.scraper_config import BATCH_SIZE

            total_batches = (len(all_product_urls) + BATCH_SIZE - 1) // BATCH_SIZE
            all_products = []

            self.logger.info(f"🚀 Starting batch processing: {total_batches} batches")

            for batch_num in range(1, total_batches + 1):
                batch_start = time.time()
                start_idx = (batch_num - 1) * BATCH_SIZE
                end_idx = min(batch_num * BATCH_SIZE, len(all_product_urls))
                batch_urls = all_product_urls[start_idx:end_idx]

                batch_products = self.scraper.scrape_products_batch(batch_urls)
                all_products.extend(batch_products)

                batch_time = time.time() - batch_start
                progress = (batch_num / total_batches) * 100

                self.logger.info(
                    f"⚡ Batch {batch_num} completed in {batch_time:.1f}s | Progress: {progress:.1f}%"
                )
                self.logger.info(
                    f"📦 Total products collected so far: {len(all_products)}"
                )

                # Note: Batch-wise Azure upload removed - only final complete dataset will be uploaded

                # Pause for system recovery
                time.sleep(2)

            # Update final statistics
            self.scraper.update_final_stats()

            # Create final result
            result = ScrapingResult(
                total_products=len(all_products),
                total_variants=len(all_products),  # One variant per product for Nanotek
                categories_scraped=["nanotek-categories"],  # Placeholder
                scraping_metadata={
                    "scraping_duration": str(datetime.now() - start_time),
                    "scraping_stats": self.scraper.scraping_stats.dict(),
                    "total_urls_discovered": len(all_product_urls),
                    "success_rate": self.scraper.scraping_stats.success_rate,
                },
                products=all_products,
            )

            # Save results locally if enabled
            if self.save_locally:
                await self._save_results(result)

            # Upload final dataset to Azure if enabled
            if self.enable_azure:
                await self._upload_final_to_azure(result)

            # Log summary
            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ SCRAPING COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 60)
            self.logger.info(f"⏱️ Duration: {datetime.now() - start_time}")
            self.logger.info(f"📦 Products scraped: {result.total_products}")
            self.logger.info(
                f"✅ Success rate: {self.scraper.scraping_stats.success_rate:.1f}%"
            )
            self.logger.info(
                f"☁️ Azure upload: {'Completed' if self.enable_azure else 'Disabled'}"
            )
            self.logger.info(
                "📝 All data collected and uploaded as single complete dataset"
            )

            # Cleanup temporary files
            self._cleanup_temp_files()

            return result

        except Exception as e:
            self.logger.error(f"Scraping failed: {e}")
            raise

    # Batch upload method removed - only final complete dataset upload is used

    async def _upload_final_to_azure(self, result: ScrapingResult):
        """Upload complete final dataset to Azure Data Lake Storage"""
        try:
            self.logger.info("\n" + "=" * 60)
            self.logger.info("☁️ UPLOADING COMPLETE DATASET TO AZURE DATA LAKE STORAGE")
            self.logger.info("=" * 60)
            self.logger.info(
                f"📊 Preparing {result.total_products:,} products for upload..."
            )

            # Create comprehensive final dataset with metadata
            final_dataset = {
                "metadata": {
                    "scrape_timestamp": datetime.utcnow().isoformat() + "Z",
                    "source_website": "nanotek.lk",
                    "scraper_version": "2.0",
                    "total_products": result.total_products,
                    "total_variants": result.total_variants,
                    "total_urls_processed": result.scraping_metadata.get(
                        "total_urls_discovered", 0
                    ),
                    "success_rate_percent": round(
                        result.scraping_metadata.get("success_rate", 0), 2
                    ),
                    "scraping_duration": result.scraping_metadata.get(
                        "scraping_duration", "unknown"
                    ),
                    "scraping_stats": result.scraping_metadata.get(
                        "scraping_stats", {}
                    ),
                    "shop_metadata": {
                        "contact_phone": "0777 292 272",
                        "website": "www.nanotek.lk",
                        "country": "Sri Lanka",
                        "currency": "LKR",
                    },
                },
                "products": [product.dict() for product in result.products],
            }

            # Convert to JSON with proper formatting
            self.logger.info("📝 Converting to JSON format...")
            json_data = json.dumps(
                final_dataset, indent=2, ensure_ascii=False, default=str
            )

            # Validate JSON structure
            try:
                json.loads(json_data)
                data_size_mb = len(json_data.encode("utf-8")) / (1024 * 1024)
                self.logger.info(f"✅ JSON validation passed ({data_size_mb:.1f} MB)")
            except json.JSONDecodeError as e:
                self.logger.error(f"❌ JSON validation failed: {e}")
                return False

            # Upload complete dataset to Azure
            self.logger.info("☁️ Uploading to Azure Data Lake Storage...")
            upload_success = upload_to_adls(
                json_data=json_data,
                source_website="nanotek.lk",
                blob_name_suffix="complete",
            )

            if upload_success:
                self.logger.info(
                    "🎉 Complete dataset successfully uploaded to Azure Data Lake Storage!"
                )
                self.logger.info(f"📊 Final Summary:")
                self.logger.info(f"   📦 Products uploaded: {result.total_products:,}")
                self.logger.info(f"   💾 Data size: {data_size_mb:.2f} MB")
                self.logger.info(
                    f"   ⏱️ Success rate: {result.scraping_metadata.get('success_rate', 0):.1f}%"
                )
                return True
            else:
                self.logger.error("❌ Failed to upload complete dataset to Azure")
                return False

        except Exception as e:
            self.logger.error(f"❌ Final Azure upload error: {e}")
            return False

    async def _save_results(self, result: ScrapingResult) -> None:
        """Save scraping results to file"""
        if not self.save_locally:
            self.logger.info("Local file saving is disabled, skipping file write")
            return

        try:
            # Extract only the products array from the result
            products_data = [product.dict() for product in result.products]

            # Save to file
            success = save_json_data(products_data, self.output_path)

            if success:
                self.logger.info(f"Results saved successfully to: {self.output_path}")
            else:
                raise Exception("Failed to save results")

        except Exception as e:
            self.logger.error(f"Error saving results: {e}")
            raise

    def _clean_existing_data(self):
        """Remove existing JSON files to ensure fresh data"""
        json_files = [
            "nanotek_products_complete.json",
            "nanotek_products_backup.json",
            "all_product_urls.json",
        ]

        cleaned_count = 0
        for file_path in json_files:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    cleaned_count += 1
                    self.logger.info(f"🗑️ Removed: {file_path}")
                except Exception as e:
                    self.logger.warning(f"⚠️ Could not remove {file_path}: {e}")

        if cleaned_count > 0:
            self.logger.info(f"✅ Cleaned {cleaned_count} existing data files")
        else:
            self.logger.info("📂 No existing data files to clean")

    def _cleanup_temp_files(self):
        """Cleanup temporary files"""
        try:
            cleanup_files = ["all_product_urls.json"]
            for cleanup_file in cleanup_files:
                if os.path.exists(cleanup_file):
                    os.remove(cleanup_file)
                    self.logger.info(f"🧹 Cleaned up: {cleanup_file}")
        except Exception as e:
            self.logger.warning(f"⚠️ Cleanup warning: {e}")


def setup_scraping_environment():
    """Setup the scraping environment (logging, directories, etc.)"""
    # Setup logging
    setup_logging(
        log_file=LOGGING_CONFIG.get("file", "nanotek_scraper.log"),
        level=LOGGING_CONFIG.get("level", "INFO"),
    )

    # Create output directory
    ensure_output_directory(OUTPUT_DIR)

    logging.info("Nanotek scraping environment initialized")


if __name__ == "__main__":
    # Basic test run
    async def test_run():
        setup_scraping_environment()
        manager = NanotekScrapingManager()
        result = await manager.run_full_scraping()
        print(f"Scraping completed: {result.total_products} products")

    asyncio.run(test_run())
