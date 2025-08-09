"""
Test script to run a controlled full scrape and see what's happening
"""
import asyncio
from scrapers.appleme.scripts.main import AppleMeScraper


async def test_full_scrape_limited():
    """Test full scrape with limited time"""
    print("Testing full scrape (limited time)...")
    scraper = AppleMeScraper()
    
    try:
        # Run full scrape
        result = await scraper.run_full_scrape()
        
        print(f"\n" + "="*60)
        print("FULL SCRAPE RESULTS:")
        print("="*60)
        print(f"Categories processed: {result.categories_processed}")
        print(f"Total products found: {result.total_products:,}")
        print(f"Successfully scraped: {result.successful_scrapes:,}")
        print(f"Failed scrapes: {result.failed_scrapes:,}")
        print(f"Success rate: {(result.successful_scrapes/result.total_products*100):.1f}%" if result.total_products > 0 else "No products found")
        print(f"Duration: {result.end_time - result.start_time}")
        
        # Show breakdown by category if available
        if hasattr(result, 'products') and result.products:
            category_breakdown = {}
            for product in result.products:
                cat = product.category_path[0] if product.category_path else "Unknown"
                category_breakdown[cat] = category_breakdown.get(cat, 0) + 1
            
            print(f"\nBreakdown by category:")
            for cat, count in sorted(category_breakdown.items(), key=lambda x: x[1], reverse=True):
                print(f"  {cat}: {count:,} products")
        
        return result
        
    except Exception as e:
        print(f"Full scrape failed: {e}")
        return None


if __name__ == "__main__":
    asyncio.run(test_full_scrape_limited())
