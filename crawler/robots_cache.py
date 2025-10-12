"""Robots.txt cache and parser for crawler compliance.

Fetches, caches, and evaluates robots.txt rules to ensure
the crawler respects website policies.
"""

import asyncio
import time
from typing import Dict, Optional, Tuple
from urllib.parse import urlsplit, urljoin
from urllib.robotparser import RobotFileParser
import httpx
import logging

logger = logging.getLogger(__name__)


class RobotsCache:
    """Cache and evaluate robots.txt rules for URLs."""
    
    def __init__(self, user_agent: str, cache_ttl_sec: int = 86400):
        """Initialize robots cache.
        
        Args:
            user_agent: User agent string for robots.txt evaluation
            cache_ttl_sec: Cache TTL in seconds (default 24 hours)
        """
        self.user_agent = user_agent
        self.cache_ttl_sec = cache_ttl_sec
        self._cache: Dict[str, Tuple[float, RobotFileParser]] = {}
        self._fetch_lock = asyncio.Lock()
        
    async def _fetch_robots(self, host: str) -> Optional[RobotFileParser]:
        """Fetch and parse robots.txt for a host.
        
        Args:
            host: Host to fetch robots.txt for
            
        Returns:
            Parsed RobotFileParser or None if fetch fails
        """
        robots_url = f"https://{host}/robots.txt"
        
        try:
            # Use conservative timeouts for robots.txt
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(connect=3.0, read=5.0, write=5.0, pool=5.0),
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
                follow_redirects=True,
                max_redirects=3
            ) as client:
                # Add standard headers
                headers = {
                    "User-Agent": self.user_agent,
                    "Accept": "text/plain, */*",
                    "Accept-Encoding": "gzip, deflate",
                }
                
                response = await client.get(robots_url, headers=headers)
                
                if response.status_code == 200:
                    # Parse robots.txt content
                    parser = RobotFileParser()
                    parser.set_url(robots_url)
                    
                    # Parse line by line (urllib.robotparser expects this format)
                    content = response.text
                    lines = content.splitlines()
                    parser.parse(lines)
                    
                    logger.info(f"Fetched robots.txt for {host}: {len(lines)} lines")
                    return parser
                    
                elif response.status_code == 404:
                    # No robots.txt means everything is allowed
                    parser = RobotFileParser()
                    parser.set_url(robots_url)
                    parser.allow_all = True
                    logger.info(f"No robots.txt found for {host} (404), allowing all")
                    return parser
                    
                else:
                    # Other status codes - be conservative and deny
                    logger.warning(f"Failed to fetch robots.txt for {host}: HTTP {response.status_code}")
                    return None
                    
        except httpx.TimeoutException:
            logger.warning(f"Timeout fetching robots.txt for {host}")
            return None
        except Exception as e:
            logger.error(f"Error fetching robots.txt for {host}: {e}")
            return None
    
    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt.
        
        Args:
            url: URL to check
            
        Returns:
            True if allowed, False if disallowed or uncertain
        """
        try:
            parts = urlsplit(url)
            host = parts.netloc.lower()
            
            if not host:
                return False
            
            # Check cache
            now = time.time()
            entry = self._cache.get(host)
            
            # Fetch if not cached or expired
            if not entry or (now - entry[0]) > self.cache_ttl_sec:
                async with self._fetch_lock:
                    # Double-check after acquiring lock
                    entry = self._cache.get(host)
                    if not entry or (now - entry[0]) > self.cache_ttl_sec:
                        parser = await self._fetch_robots(host)
                        if parser is None:
                            # Failed to fetch - be conservative and deny
                            return False
                        self._cache[host] = (now, parser)
                        entry = self._cache[host]
            
            # Check if URL is allowed
            parser = entry[1]
            
            # Check for our specific user agent
            allowed = parser.can_fetch(self.user_agent, url)
            
            if not allowed:
                logger.debug(f"Robots.txt disallows {url} for user-agent {self.user_agent}")
            
            return allowed
            
        except Exception as e:
            logger.error(f"Error checking robots.txt for {url}: {e}")
            # Be conservative on errors
            return False
    
    async def get_crawl_delay(self, host: str) -> Optional[float]:
        """Get crawl delay for a host if specified in robots.txt.
        
        Args:
            host: Host to check
            
        Returns:
            Crawl delay in seconds or None if not specified
        """
        try:
            # Check cache
            now = time.time()
            entry = self._cache.get(host.lower())
            
            if not entry or (now - entry[0]) > self.cache_ttl_sec:
                # Need to fetch first
                test_url = f"https://{host}/"
                await self.is_allowed(test_url)
                entry = self._cache.get(host.lower())
            
            if entry:
                parser = entry[1]
                # Try to get crawl delay (not all parsers support this)
                if hasattr(parser, 'crawl_delay'):
                    delay = parser.crawl_delay(self.user_agent)
                    if delay is not None:
                        logger.info(f"Crawl-delay for {host}: {delay}s")
                        return float(delay)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting crawl delay for {host}: {e}")
            return None
    
    def clear_cache(self):
        """Clear the robots.txt cache."""
        self._cache.clear()
        logger.info("Cleared robots.txt cache")
    
    def get_cache_stats(self) -> dict:
        """Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        now = time.time()
        valid = 0
        expired = 0
        
        for host, (timestamp, _) in self._cache.items():
            if (now - timestamp) <= self.cache_ttl_sec:
                valid += 1
            else:
                expired += 1
        
        return {
            "total_entries": len(self._cache),
            "valid_entries": valid,
            "expired_entries": expired,
            "hosts_cached": list(self._cache.keys())
        }
