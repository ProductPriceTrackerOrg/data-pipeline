"""
Performance optimization utilities for the scraper
"""
import asyncio
import time
from typing import List, Dict, Any, Callable, Optional
from config.scraper_config import ScraperConfig
from utils.scraper_utils import ScraperUtils


class ScrapingMode:
    """Different scraping modes for different performance needs"""
    CONSERVATIVE = "conservative"  # Slow but safe
    BALANCED = "balanced"         # Default mode
    AGGRESSIVE = "aggressive"     # Fast but might trigger rate limits


class PerformanceOptimizer:
    """Optimizes scraping performance based on server response patterns"""
    
    def __init__(self, mode: str = ScrapingMode.BALANCED):
        self.mode = mode
        self.logger = ScraperUtils.setup_logging()
        self.success_rate = 1.0
        self.response_times = []
        self.rate_limit_count = 0
        
    def get_optimized_delays(self) -> tuple:
        """Get optimized delays based on current mode and performance"""
        if self.mode == ScrapingMode.CONSERVATIVE:
            return (1.5, 4.0)
        elif self.mode == ScrapingMode.AGGRESSIVE:
            if self.success_rate > 0.9 and self.rate_limit_count == 0:
                return (0.2, 0.8)
            else:
                return (0.8, 2.0)
        else:  # BALANCED
            if self.success_rate > 0.95:
                return (0.5, 1.5)
            elif self.success_rate > 0.8:
                return (1.0, 2.5)
            else:
                return (2.0, 4.0)
    
    def get_optimized_concurrency(self) -> int:
        """Get optimized concurrency based on performance"""
        base_concurrency = ScraperConfig.MAX_CONCURRENT_REQUESTS
        
        if self.mode == ScrapingMode.CONSERVATIVE:
            return max(2, base_concurrency // 2)
        elif self.mode == ScrapingMode.AGGRESSIVE:
            if self.rate_limit_count == 0:
                return base_concurrency * 2
            else:
                return base_concurrency
        else:  # BALANCED
            if self.success_rate > 0.9:
                return base_concurrency
            else:
                return max(3, base_concurrency // 2)
    
    def record_response(self, success: bool, response_time: float, rate_limited: bool = False):
        """Record response for performance optimization"""
        self.response_times.append(response_time)
        if len(self.response_times) > 100:  # Keep only last 100
            self.response_times.pop(0)
        
        if rate_limited:
            self.rate_limit_count += 1
        
        # Calculate success rate over last 50 requests
        if hasattr(self, 'recent_results'):
            self.recent_results.append(success)
            if len(self.recent_results) > 50:
                self.recent_results.pop(0)
            self.success_rate = sum(self.recent_results) / len(self.recent_results)
        else:
            self.recent_results = [success]
            self.success_rate = 1.0 if success else 0.0


class BatchProcessor:
    """Processes items in optimized batches"""
    
    def __init__(self, optimizer: PerformanceOptimizer):
        self.optimizer = optimizer
        self.logger = ScraperUtils.setup_logging()
    
    async def process_in_batches(self, items: List[Any], 
                               process_func: Callable,
                               batch_size: Optional[int] = None) -> List[Any]:
        """Process items in optimized batches"""
        if not batch_size:
            batch_size = ScraperConfig.BATCH_SIZE
        
        results = []
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            self.logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} items)")
            
            try:
                batch_results = await process_func(batch)
                results.extend(batch_results)
                
                # Add small delay between batches to be respectful
                if batch_num < total_batches:
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {e}")
        
        return results


class RateLimitRecovery:
    """Handles recovery from rate limiting"""
    
    def __init__(self):
        self.logger = ScraperUtils.setup_logging()
        self.backoff_multiplier = 1.0
        self.last_rate_limit_time = 0
    
    async def handle_rate_limit(self):
        """Handle rate limit with intelligent backoff"""
        current_time = time.time()
        
        # If we got rate limited recently, increase backoff
        if current_time - self.last_rate_limit_time < 60:
            self.backoff_multiplier = min(self.backoff_multiplier * 1.5, 5.0)
        else:
            self.backoff_multiplier = max(self.backoff_multiplier * 0.8, 1.0)
        
        wait_time = ScraperConfig.RATE_LIMIT_DELAY * self.backoff_multiplier
        self.logger.warning(f"Rate limited. Waiting {wait_time:.1f}s (multiplier: {self.backoff_multiplier:.1f})")
        
        self.last_rate_limit_time = current_time
        await asyncio.sleep(wait_time)
    
    def reset_backoff(self):
        """Reset backoff after successful requests"""
        self.backoff_multiplier = max(self.backoff_multiplier * 0.9, 1.0)
