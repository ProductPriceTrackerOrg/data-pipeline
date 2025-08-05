import asyncio
import aiohttp
import random
import time
from typing import Optional, List
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class RequestResult:
    url: str
    content: Optional[str]
    status_code: int
    success: bool
    error: Optional[str] = None


class RateLimitedHttpClient:
    def __init__(self, 
                 concurrent_requests: int = 5,
                 delay_between_requests: float = 0.5,
                 max_retries: int = 3,
                 backoff_factor: float = 2.0):
        
        self.concurrent_requests = concurrent_requests
        self.delay_between_requests = delay_between_requests
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = None
        self.semaphore = asyncio.Semaphore(concurrent_requests)
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ]
        
        # Rate limiting
        self.last_request_time = 0
        self.request_count = 0
        self.rate_limit_delay = 1.0  # Initial delay for 429 errors

    async def __aenter__(self):
        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            ttl_dns_cache=300,
            use_dns_cache=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def fetch_url(self, url: str) -> RequestResult:
        """Fetch a single URL with rate limiting and retry logic"""
        async with self.semaphore:
            await self._respect_rate_limit()
            
            for attempt in range(self.max_retries + 1):
                try:
                    headers = {
                        'User-Agent': random.choice(self.user_agents),
                        'Referer': 'https://www.simplytek.lk/',
                    }
                    
                    async with self.session.get(url, headers=headers) as response:
                        if response.status == 429:
                            # Handle rate limiting
                            await self._handle_rate_limit(attempt)
                            continue
                        
                        if response.status == 200:
                            content = await response.text()
                            self.request_count += 1
                            return RequestResult(
                                url=url,
                                content=content,
                                status_code=response.status,
                                success=True
                            )
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            if attempt == self.max_retries:
                                return RequestResult(
                                    url=url,
                                    content=None,
                                    status_code=response.status,
                                    success=False,
                                    error=f"HTTP {response.status}"
                                )
                
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                    if attempt == self.max_retries:
                        return RequestResult(
                            url=url,
                            content=None,
                            status_code=0,
                            success=False,
                            error="Timeout"
                        )
                    await asyncio.sleep(self.backoff_factor ** attempt)
                
                except Exception as e:
                    logger.error(f"Error fetching {url}: {str(e)}")
                    if attempt == self.max_retries:
                        return RequestResult(
                            url=url,
                            content=None,
                            status_code=0,
                            success=False,
                            error=str(e)
                        )
                    await asyncio.sleep(self.backoff_factor ** attempt)
            
            return RequestResult(
                url=url,
                content=None,
                status_code=0,
                success=False,
                error="Max retries exceeded"
            )

    async def fetch_multiple(self, urls: List[str]) -> List[RequestResult]:
        """Fetch multiple URLs concurrently"""
        tasks = [self.fetch_url(url) for url in urls]
        return await asyncio.gather(*tasks, return_exceptions=False)

    async def _respect_rate_limit(self):
        """Ensure we don't exceed rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.delay_between_requests:
            await asyncio.sleep(self.delay_between_requests - time_since_last)
        
        self.last_request_time = time.time()

    async def _handle_rate_limit(self, attempt: int):
        """Handle 429 rate limit responses"""
        delay = self.rate_limit_delay * (self.backoff_factor ** attempt)
        jitter = random.uniform(0.1, 0.5)  # Add jitter
        total_delay = delay + jitter
        
        logger.warning(f"Rate limited (429), waiting {total_delay:.2f}s")
        await asyncio.sleep(total_delay)
        
        # Increase base delay for future requests
        self.rate_limit_delay = min(self.rate_limit_delay * 1.2, 10.0)
        self.delay_between_requests = min(self.delay_between_requests * 1.1, 2.0)

    def get_stats(self) -> dict:
        """Get client statistics"""
        return {
            'total_requests': self.request_count,
            'current_rate_limit_delay': self.rate_limit_delay,
            'current_request_delay': self.delay_between_requests
        }