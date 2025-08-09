"""
Quick performance test for the turbo scraper optimizations
"""
import asyncio
import time
from turbo_scraper import run_turbo_complete_scrape


async def quick_speed_test():
    """Run a quick test to validate speed improvements"""
    print("🔬 TURBO SCRAPER SPEED TEST")
    print("=" * 50)
    print("Testing a small subset to validate optimizations...")
    print()
    
    from scrapers.appleme.scripts.main import AppleMeScraper
    scraper = AppleMeScraper()
    
    # Test category fetching speed
    start_time = time.time()
    result = await scraper.run_category_scrape(['MOBILE PHONES'])  # Small category
    end_time = time.time()
    
    duration = end_time - start_time
    products_per_second = result.successful_scrapes / duration if duration > 0 else 0
    products_per_minute = products_per_second * 60
    
    print(f"Test Results:")
    print(f"   Category: MOBILE PHONES")
    print(f"   Products: {result.successful_scrapes}/{result.total_products}")
    print(f"   Duration: {duration:.1f} seconds")
    print(f"   Speed: {products_per_minute:.1f} products/minute")
    print(f"   Success Rate: {(result.successful_scrapes/result.total_products)*100:.1f}%")
    print()
    
    # Performance projections
    estimated_full_time_minutes = (2300 / products_per_minute) if products_per_minute > 0 else 0
    
    print(f"Projections for full scrape (~2,300 products):")
    print(f"   Estimated time: {estimated_full_time_minutes:.1f} minutes")
    
    if estimated_full_time_minutes < 6:
        print("   EXCELLENT! This should be very fast!")
    elif estimated_full_time_minutes < 10:
        print("   GREAT! Good improvement over 10+ minutes!")
    else:
        print("   Still room for optimization...")
    
    print("=" * 50)
    
    return products_per_minute


if __name__ == "__main__":
    try:
        asyncio.run(quick_speed_test())
    except Exception as e:
        print(f"Test failed: {e}")
