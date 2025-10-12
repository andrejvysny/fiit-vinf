"""HTTP fetcher for crawler with retries and rate limiting.

Downloads HTML pages with exponential backoff, respects rate limits,
and handles common HTTP errors gracefully.
"""

import asyncio
import logging
import time
from typing import Optional, Dict, Tuple
import httpx
from ..config import CrawlerConfig

logger = logging.getLogger(__name__)


class CrawlerFetcher:
    """HTTP client for fetching web pages with retry logic."""
    
    def __init__(self, config: CrawlerConfig):
        """Initialize fetcher.
        
        Args:
            config: Crawler configuration
        """
        self.config = config
        self.user_agent = config.user_agent
        self.accept_language = config.accept_language
        self.accept_encoding = config.accept_encoding
        
        # Build timeouts from config
        self.timeout = httpx.Timeout(
            connect=config.limits.connect_timeout_ms / 1000.0,
            read=config.limits.read_timeout_ms / 1000.0,
            write=5.0,
            pool=5.0
        )
        
        self.max_retries = config.limits.max_retries
        self.backoff_base_ms = config.limits.backoff_base_ms
        self.backoff_cap_ms = config.limits.backoff_cap_ms
        
        # Rate limiting
        self.req_per_sec = config.limits.req_per_sec
        self._last_request_time = 0.0
        self._rate_lock = asyncio.Lock()
        
        # Metrics
        self.stats = {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "bytes_downloaded": 0
        }
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limit by waiting if necessary."""
        async with self._rate_lock:
            now = time.time()
            min_interval = 1.0 / self.req_per_sec
            elapsed = now - self._last_request_time
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)
            
            self._last_request_time = time.time()
    
    async def fetch(self, url: str) -> Tuple[bool, Optional[str], Optional[Dict]]:
        """Fetch URL with retries and rate limiting.
        
        Args:
            url: URL to fetch
            
        Returns:
            Tuple of (success: bool, html: Optional[str], metadata: Optional[Dict])
            
            metadata includes:
                - status_code: int
                - content_type: str
                - content_length: int
                - latency_ms: float
        """
        # Wait for rate limit
        await self._wait_for_rate_limit()
        
        self.stats["requests"] += 1
        
        # Retry loop
        for attempt in range(self.max_retries + 1):
            try:
                start_time = time.time()
                
                async with httpx.AsyncClient(
                    timeout=self.timeout,
                    follow_redirects=True,
                    max_redirects=5,
                    limits=httpx.Limits(
                        max_keepalive_connections=10,
                        max_connections=20
                    )
                ) as client:
                    headers = {
                        "User-Agent": self.user_agent,
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": self.accept_language,
                        "Accept-Encoding": self.accept_encoding,
                        "Connection": "keep-alive",
                    }
                    
                    response = await client.get(url, headers=headers)
                    
                    latency_ms = (time.time() - start_time) * 1000
                    
                    # Check status code
                    if response.status_code == 200:
                        html = response.text
                        self.stats["successes"] += 1
                        self.stats["bytes_downloaded"] += len(html)
                        
                        metadata = {
                            "status_code": response.status_code,
                            "content_type": response.headers.get("content-type", ""),
                            "content_length": len(html),
                            "latency_ms": latency_ms
                        }
                        
                        logger.debug(f"Fetched {url}: {len(html)} bytes in {latency_ms:.0f}ms")
                        return (True, html, metadata)
                    
                    elif response.status_code == 404:
                        # Not found - don't retry
                        logger.debug(f"404 Not Found: {url}")
                        self.stats["failures"] += 1
                        return (False, None, {"status_code": 404, "latency_ms": latency_ms})
                    
                    elif response.status_code == 403:
                        # Forbidden - possibly rate limited or blocked
                        logger.warning(f"403 Forbidden: {url}")
                        self.stats["failures"] += 1
                        return (False, None, {"status_code": 403, "latency_ms": latency_ms})
                    
                    elif response.status_code == 429:
                        # Rate limited - wait and retry
                        logger.warning(f"429 Rate Limited: {url}, attempt {attempt + 1}/{self.max_retries + 1}")
                        self.stats["retries"] += 1
                        
                        # Check for Retry-After header
                        retry_after = response.headers.get("Retry-After")
                        if retry_after:
                            try:
                                wait_time = int(retry_after)
                            except ValueError:
                                wait_time = self._calculate_backoff(attempt)
                        else:
                            wait_time = self._calculate_backoff(attempt)
                        
                        logger.info(f"Waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    else:
                        # Other error - retry with backoff
                        logger.warning(f"HTTP {response.status_code}: {url}, attempt {attempt + 1}/{self.max_retries + 1}")
                        self.stats["retries"] += 1
                        
                        if attempt < self.max_retries:
                            wait_time = self._calculate_backoff(attempt) / 1000.0
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            self.stats["failures"] += 1
                            return (False, None, {"status_code": response.status_code, "latency_ms": latency_ms})
            
            except httpx.TimeoutException:
                logger.warning(f"Timeout fetching {url}, attempt {attempt + 1}/{self.max_retries + 1}")
                self.stats["retries"] += 1
                
                if attempt < self.max_retries:
                    wait_time = self._calculate_backoff(attempt) / 1000.0
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.stats["failures"] += 1
                    return (False, None, {"error": "timeout"})
            
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}, attempt {attempt + 1}/{self.max_retries + 1}")
                self.stats["retries"] += 1
                
                if attempt < self.max_retries:
                    wait_time = self._calculate_backoff(attempt) / 1000.0
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.stats["failures"] += 1
                    return (False, None, {"error": str(e)})
        
        # Should not reach here
        self.stats["failures"] += 1
        return (False, None, {"error": "max_retries_exceeded"})
    
    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff in milliseconds.
        
        Args:
            attempt: Retry attempt number (0-indexed)
            
        Returns:
            Backoff time in milliseconds
        """
        backoff = self.backoff_base_ms * (2 ** attempt)
        return min(backoff, self.backoff_cap_ms)
    
    def get_stats(self) -> Dict:
        """Get fetcher statistics.
        
        Returns:
            Dictionary with request counts and metrics
        """
        return dict(self.stats)
