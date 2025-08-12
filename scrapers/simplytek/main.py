#!/usr/bin/env python3
"""
SimplyTek Web Scraper - Simplified Version
Scrapes product data from SimplyTek and saves as JSON

Usage:
    python main.py
"""

import asyncio
import logging
import sys
from datetime import datetime

# Add the current directory to the path to allow imports
sys.path.append('.')

from scripts.product_scraper_manager import ScrapingManager, setup_scraping_environment


def print_banner():
    """Print application banner"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                    SimplyTek Web Scraper                     ║
║                  Simple Product Data Scraper                 ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def main():
    """Main entry point - scrape all products and save as JSON"""
    print_banner()
    print("Starting SimplyTek product scraping...")
    print(f"Scraping started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Setup environment (logging, directories, etc.)
        setup_scraping_environment()
        
        # Create scraping manager
        manager = ScrapingManager()
        
        # Run full scraping
        print("\n🔍 Scraping all products from SimplyTek...")
        result = await manager.run_full_scraping()
        
        # Display results
        print(f"\n{'='*60}")
        print(" SCRAPING COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f" Products scraped: {result.total_products:,}")
        print(f" Variants scraped: {result.total_variants:,}")
        print(f" Categories processed: {len(result.categories_scraped)}")
        print(f" Duration: {result.scraping_metadata.get('scraping_duration', 'N/A')}")
        print(f" Data saved to: {manager.output_path}")

        return 0
        
    except KeyboardInterrupt:
        print("\n  Scraping interrupted by user.")
        return 1
    except Exception as e:
        print(f"\n Scraping failed: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nOperation interrupted by user.")
        exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        exit(1)