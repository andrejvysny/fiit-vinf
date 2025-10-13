"""File-based robots.txt cache.

Replaces in-memory cache with JSONL file storage for persistence.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional, Dict
from urllib.robotparser import RobotFileParser
import httpx

logger = logging.getLogger(__name__)


class FileRobotsCache:
    """File-based robots.txt cache."""
    
    def __init__(self, cache_path: str, user_agent: str, cache_ttl_sec: int = 86400):
        """Initialize robots cache.
        
        Args:
            cache_path: Path to robots_cache.jsonl file
            user_agent: User agent string for robots.txt checking
            cache_ttl_sec: Cache TTL in seconds (default 24 hours)
        """
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.user_agent = user_agent
        self.cache_ttl_sec = cache_ttl_sec
        
        # In-memory cache for performance
        self._cache: Dict[str, Dict] = {}
        
        # Load existing cache
        self._load()
        
        logger.info(f"Robots cache initialized with {len(self._cache)} entries")
    
    def _load(self):
        """Load robots cache from disk."""
        if not self.cache_path.exists():
            logger.info("No existing robots cache found - starting fresh")
            return
        
        try:
            with open(self.cache_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        entry = json.loads(line)
                        domain = entry.get('domain')
                        if domain:
                            self._cache[domain] = entry
                    except Exception as e:
                        logger.error(f"Failed to parse robots cache line: {e}")
            
            logger.info(f"Loaded {len(self._cache)} robots.txt entries")
            
        except Exception as e:
            logger.error(f"Failed to load robots cache: {e}")
            self._cache = {}
    
    def _persist_entry(self, domain: str, entry: Dict):
        """Persist a single cache entry to disk (append).
        
        Args:
            domain: Domain name
            entry: Cache entry to persist
        """
        try:
            with open(self.cache_path, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            logger.error(f"Failed to persist robots cache entry for {domain}: {e}")
    
    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt (interface compatibility).
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is allowed, False if disallowed
        """
        return await self.can_fetch(url)
    
    async def can_fetch(self, url: str) -> bool:
        """Check if URL can be fetched according to robots.txt.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL can be fetched, False if disallowed
        """
        from urllib.parse import urlsplit
        
        parts = urlsplit(url)
        domain = parts.netloc.lower()
        
        # Check if we have cached entry
        now = time.time()
        cached = self._cache.get(domain)
        
        if cached:
            # Check if cache is still valid
            fetched_at = cached.get('fetched_at', 0)
            expires_at = cached.get('expires_at', 0)
            
            if now < expires_at:
                # Cache hit - use cached result
                content = cached.get('content', '')
                return self._check_robots_content(content, url)
            else:
                logger.debug(f"Robots cache expired for {domain}")
        
        # Cache miss or expired - fetch robots.txt
        logger.debug(f"Fetching robots.txt for {domain}")
        
        robots_url = f"{parts.scheme}://{domain}/robots.txt"
        content = await self._fetch_robots(robots_url)
        
        # Create cache entry
        entry = {
            'domain': domain,
            'content': content,
            'fetched_at': now,
            'expires_at': now + self.cache_ttl_sec
        }
        
        # Update cache
        self._cache[domain] = entry
        self._persist_entry(domain, entry)
        
        # Check permissions
        return self._check_robots_content(content, url)
    
    async def _fetch_robots(self, robots_url: str) -> str:
        """Fetch robots.txt content.
        
        Args:
            robots_url: URL to robots.txt
            
        Returns:
            robots.txt content (empty string on error)
        """
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(robots_url)
                
                if response.status_code == 200:
                    return response.text
                else:
                    logger.warning(f"robots.txt returned {response.status_code} for {robots_url}")
                    return ""
                    
        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt from {robots_url}: {e}")
            # On error, deny access to be conservative
            return "User-agent: *\nDisallow: /"
    
    def _check_robots_content(self, content: str, url: str) -> bool:
        """Check if URL is allowed by robots.txt content.
        
        Args:
            content: robots.txt content
            url: URL to check
            
        Returns:
            True if allowed, False if disallowed
        """
        if not content:
            # No robots.txt or empty - allow by default
            return True
        
        try:
            parser = RobotFileParser()
            parser.parse(content.splitlines())
            return parser.can_fetch(self.user_agent, url)
        except Exception as e:
            logger.error(f"Failed to parse robots.txt: {e}")
            # On parse error, deny to be safe
            return False
    
    def persist(self):
        """Persist entire cache to disk (full rewrite).
        
        This is called on shutdown to ensure all entries are saved.
        """
        try:
            with open(self.cache_path, 'w') as f:
                for entry in self._cache.values():
                    f.write(json.dumps(entry) + '\n')
            
            logger.info(f"Persisted robots cache with {len(self._cache)} entries")
            
        except Exception as e:
            logger.error(f"Failed to persist robots cache: {e}")
    
    def close(self):
        """Close cache and persist final state."""
        logger.info("Closing robots cache...")
        self.persist()
