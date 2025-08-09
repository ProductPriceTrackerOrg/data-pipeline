"""
Run a complete scrape with progress monitoring
"""
import asyncio
from datetime import datetime
from scrapers.appleme.scripts.main import AppleMeScraper


async def run_complete_scrape():
    """Run a complete scrape with detailed progress monitoring"""
    print("Starting COMPLETE AppleMe.lk scraping...")
    print("Expected time: 12-15 minutes for ~2,300 products")
    print("Progress will be shown every 50 products")
    print("-" * 60)
    
    scraper = AppleMeScraper()
    
    try:
        # Run full scraping process
        result = await scraper.run_full_scrape()
        
        print("\n" + "COMPLETE SCRAPE FINISHED!")
        print("=" * 60)
        print(f"Start Time: {result.start_time.strftime('%H:%M:%S')}")
        print(f"End Time: {result.end_time.strftime('%H:%M:%S')}")
        print(f"Total Duration: {result.end_time - result.start_time}")
        print(f"Categories Processed: {result.categories_processed}")
        print(f"Total Products Found: {result.total_products:,}")
        print(f"Successfully Scraped: {result.successful_scrapes:,}")
        print(f"Failed Scrapes: {result.failed_scrapes:,}")
        
        if result.total_products > 0:
            success_rate = (result.successful_scrapes / result.total_products) * 100
            print(f"Success Rate: {success_rate:.1f}%")
        
        print(f"Data saved to: scraped_data/appleme_products.json")
        
        # Show category breakdown if available
        if hasattr(result, 'products') and result.products:
            category_breakdown = {}
            for product in result.products:
                cat = product.category_path[0] if product.category_path else "Unknown"
                category_breakdown[cat] = category_breakdown.get(cat, 0) + 1
            
            print(f"\nBreakdown by category:")
            for cat, count in sorted(category_breakdown.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / result.successful_scrapes) * 100
                print(f"  {cat:<20}: {count:,} products ({percentage:.1f}%)")
        
        print("=" * 60)
        return result
        
    except Exception as e:
        print(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    print("AppleMe.lk Complete Product Scraper")
    print("This will scrape ALL products from ALL categories.")
    print("\nPress Ctrl+C to interrupt if needed...\n")
    
    try:
        asyncio.run(run_complete_scrape())
    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
        print("Partial results may be saved in scraped_data/")
    except Exception as e:
        print(f"\nFatal error: {e}")
        print("Check logs for more details.")
