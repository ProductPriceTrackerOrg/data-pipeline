"""
Nanotek Web Scraper - Enhanced Data Pipeline
Scrapes product data from Nanotek.lk and uploads to Azure Data Lake Storage (ADLS)

Usage:
    python main.py
"""

import logging
import sys
import os
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/nanotek_scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

from utils.scraper_utils import print_banner, validate_azure_configuration
from config.scraper_config import AZURE_CONFIG


def main():
    """Main entry point for Nanotek scraper"""
    print_banner()
    print("🚀 Starting Nanotek enhanced data pipeline...")
    print(f"⏰ Pipeline started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Setup logging
    logging.info("Nanotek scraping environment initialized")

    # Validate Azure configuration if enabled
    enable_azure = AZURE_CONFIG.get("enable_upload", True)
    if enable_azure:
        azure_valid = validate_azure_configuration()
        if not azure_valid:
            logging.warning("Azure upload will be disabled due to configuration issues")
            enable_azure = False

    try:
        # Use the modular scraper manager for better organization
        print("📝 Using modular scraper implementation...")
        print(
            "💡 This will collect all products and upload as single complete dataset to Azure"
        )

        # Import and use the modular scraper manager
        import asyncio
        from scripts.product_scraper_manager import (
            NanotekScrapingManager,
            setup_scraping_environment,
        )

        # Setup scraping environment
        setup_scraping_environment()

        # Initialize scraper manager with Azure support
        manager = NanotekScrapingManager(enable_azure=enable_azure, save_locally=True)

        # Run the scraper asynchronously
        result = asyncio.run(manager.run_full_scraping(resume=False))

        # Display final results
        if result:
            print(f"\n{'='*60}")
            print(" ✅ SCRAPING COMPLETED SUCCESSFULLY")
            print(f"{'='*60}")
            print(f" 📦 Products scraped: {result.total_products:,}")
            print(f" 🔄 Total variants: {result.total_variants:,}")
            print(
                f" ✅ Success rate: {result.scraping_metadata.get('success_rate', 0):.1f}%"
            )
            print(
                f" ⏱️ Duration: {result.scraping_metadata.get('scraping_duration', 'unknown')}"
            )
            print(f" ☁️ Azure upload: {'Enabled' if enable_azure else 'Disabled'}")
            print(f"{'='*60}")
            print("📝 All data collected and uploaded as single complete dataset")

            return result.products
        else:
            print("❌ No products were scraped")
            return []

    except KeyboardInterrupt:
        print("\n⏸️ Scraping interrupted by user")
        print("💡 You can resume later by setting resume=True")
        return None
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}", exc_info=True)
        print(f"❌ Scraping failed: {e}")
        return None


if __name__ == "__main__":
    try:
        result = main()
        if result:
            print(f"\n🎉 Scraping completed successfully!")
            print(f"📦 Final count: {len(result)} products scraped and uploaded")
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)
