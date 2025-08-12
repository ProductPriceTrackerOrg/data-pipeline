"""
Test script to verify scraping performance improvements
"""
import asyncio
import time
from scrapers.appleme.scripts.main import AppleMeScraper
from scrapers.appleme.scripts.performance_optimizer import PerformanceOptimizer, ScrapingMode


async def test_category_fetch():
    """Test if categories can be fetched successfully"""
    print("Testing category fetching...")
    scraper = AppleMeScraper()
    
    from scrapers.appleme.utils.scraper_utils import AsyncRequestManager
    async with AsyncRequestManager() as request_manager:
        categories = await scraper.category_scraper.get_main_categories(request_manager)
        
        if categories:
            print(f"Successfully fetched {len(categories)} categories:")
            for cat in categories[:5]:  # Show first 5
                print(f"  - {cat['name']}: {cat['url']}")
            if len(categories) > 5:
                print(f"  ... and {len(categories) - 5} more")
        else:
            print("No categories found")
        
        return len(categories) > 0


async def test_single_category():
    """Test scraping a single category"""
    print("\nTesting single category scraping...")
    scraper = AppleMeScraper()
    
    # Test with a specific category
    result = await scraper.run_category_scrape(['MOBILE PHONES'])
    
    if result.successful_scrapes > 0:
        print(f"Successfully scraped {result.successful_scrapes} products from MOBILE PHONES")
        print(f"   Duration: {result.end_time - result.start_time}")
        print(f"   Success rate: {(result.successful_scrapes / result.total_products * 100):.1f}%")
    else:
        print("No products scraped successfully")
    
    return result.successful_scrapes > 0


async def performance_test():
    """Run performance tests"""
    print("="*60)
    print("APPLEME.LK SCRAPER PERFORMANCE TEST")
    print("="*60)
    
    start_time = time.time()
    
    # Test 1: Category fetching
    categories_ok = await test_category_fetch()
    
    if categories_ok:
        # Test 2: Single category scraping
        scraping_ok = await test_single_category()
        
        if scraping_ok:
            print("\nAll tests passed! Scraper is working properly.")
        else:
            print("\nCategory fetching works, but product scraping failed.")
    else:
        print("\nCategory fetching failed. Check URL and selectors.")
    
    total_time = time.time() - start_time
    print(f"\nTotal test time: {total_time:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(performance_test())
