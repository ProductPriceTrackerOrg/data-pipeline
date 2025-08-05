#!/usr/bin/env python3
"""
Production runner for SimplyTek scraper
Usage: python main.py [options]
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
import os

# Add the parent directory to sys.path to enable absolute imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Use absolute imports for the modules
from scrapers.simplytek.scripts.scraper import SimplyTekScraper
from scrapers.simplytek.models import ScrapingResult

# Setup logging
def setup_logging(log_level: str = "INFO", log_file: str = None):
    """Setup logging configuration"""
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Setup file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


async def run_scraper(args):
    """Run the scraper with given arguments"""
    logger = logging.getLogger(__name__)
    
    # Initialize scraper
    scraper = SimplyTekScraper(
        max_pages=args.max_pages,
        concurrent_requests=args.concurrent_requests
    )
    
    logger.info(f"Starting scraper with {args.concurrent_requests} concurrent requests")
    logger.info(f"Maximum pages to scrape: {args.max_pages}")
    
    try:
        if args.sample:
            # Run sample scraping
            logger.info(f"Running sample scraping ({args.sample_size} products)")
            result = await scraper.scrape_sample(args.sample_size)
        else:
            # Run full scraping
            logger.info("Running full product scraping")
            result = await scraper.scrape_all_products()
        
        # Print results summary
        print_results_summary(result)
        
        # Save results
        if result.products:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = args.output or f"simplytek_products_{timestamp}.json"
            scraper.save_results(output_file)
            logger.info(f"Results saved to: {output_file}")
        
        # Save errors if any
        if result.errors:
            error_file = f"scraping_errors_{timestamp}.txt"
            with open(error_file, 'w') as f:
                f.write(f"Scraping Errors - {datetime.now()}\n")
                f.write("=" * 50 + "\n")
                for error in result.errors:
                    f.write(f"{error}\n")
            logger.info(f"Errors saved to: {error_file}")
        
        return result.success
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        return False

def print_results_summary(result: ScrapingResult):
    """Print detailed results summary"""
    print("\n" + "=" * 60)
    print("SCRAPING RESULTS SUMMARY")
    print("=" * 60)
    
    print(f"Status: {'✓ SUCCESS' if result.success else '✗ FAILED'}")
    print(f"Products scraped: {result.total_products}")
    print(f"Pages processed: {result.pages_scraped}")
    print(f"Execution time: {result.execution_time:.2f} seconds")
    print(f"Errors encountered: {len(result.errors)}")
    
    if result.total_products > 0:
        avg_time_per_product = result.execution_time / result.total_products
        print(f"Average time per product: {avg_time_per_product:.2f} seconds")
    
    if result.errors:
        print(f"\nRecent errors (showing last 5):")
        for error in result.errors[-5:]:
            print(f"  • {error}")
    
    print("=" * 60)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="SimplyTek High-Speed Web Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          # Full scraping
  python main.py --sample --sample-size 10  # Sample 10 products
  python main.py --max-pages 5            # Limit to 5 pages
  python main.py --concurrent-requests 8  # Use 8 concurrent requests
  python main.py --output my_products.json # Custom output file
        """
    )
    
    # Scraping options
    parser.add_argument('--max-pages', type=int, default=25,
                       help='Maximum pages to scrape (default: 25)')
    parser.add_argument('--concurrent-requests', type=int, default=5,
                       help='Number of concurrent requests (default: 5)')
    parser.add_argument('--sample', action='store_true',
                       help='Run sample scraping instead of full scraping')
    parser.add_argument('--sample-size', type=int, default=10,
                       help='Number of products for sample scraping (default: 10)')
    
    # Output options
    parser.add_argument('--output', '-o', type=str,
                       help='Output JSON file path')
    
    # Logging options
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Log level (default: INFO)')
    parser.add_argument('--log-file', type=str,
                       help='Log file path (optional)')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    
    # Validate arguments
    if args.concurrent_requests > 10:
        print("    Warning: High concurrent requests may trigger rate limiting")
        print("    Consider using --concurrent-requests 5 or lower")

    if args.max_pages > 30:
        print("    Warning: High page count may take significant time")

    # Run scraper
    try:
        success = asyncio.run(run_scraper(args))
        
        if success:
            print("\n Scraping completed successfully!")
            sys.exit(0)
        else:
            print("\n Scraping failed or was interrupted")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()