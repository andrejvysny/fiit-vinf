"""Unified URL crawling policy with robots.txt compliance.

Merges FileRobotsCache and UrlPolicy into a single module that:
- Validates URLs against allow/deny patterns
- Checks host permissions
- Enforces robots.txt compliance
- Classifies page types
- Manages robots.txt cache (file-based JSONL)
"""

import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser
import httpx

from .url_tools import canonicalize, is_github_url

logger = logging.getLogger(__name__)


class CrawlPolicy:
    """Unified URL policy enforcement with robots.txt compliance.
    
    Combines URL pattern validation and robots.txt checking into a single
    component for streamlined policy enforcement.
    """
    
    def __init__(self, config: Any, cache_path: str, user_agent: str, cache_ttl_sec: int = 86400):
        """Initialize crawl policy.
        
        Args:
            config: Crawler configuration (any object with .scope attributes)
            cache_path: Path to robots_cache.jsonl file
            user_agent: User agent string for robots.txt checking
            cache_ttl_sec: Cache TTL in seconds (default 24 hours)
        """
        # Configuration
        self.config = config
        self.user_agent = user_agent
        self.cache_ttl_sec = cache_ttl_sec
        
        # Compile regex patterns for efficiency
        self.allow_patterns = [re.compile(pattern) for pattern in getattr(config.scope, 'allow_patterns', [])]
        self.deny_patterns = [re.compile(pattern) for pattern in getattr(config.scope, 'deny_patterns', [])]
        
        # Convert host lists to sets for O(1) lookup
        self.allowed_hosts = set(getattr(config.scope, 'allowed_hosts', []))
        self.denied_subdomains = set(getattr(config.scope, 'denied_subdomains', []))
        
        # Robots.txt cache setup
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory robots cache for performance
        self._robots_cache: Dict[str, Dict] = {}
        
        # Load existing robots cache
        self._load_robots_cache()
        
        logger.info(
            f"Crawl policy initialized: "
            f"{len(self.allow_patterns)} allow patterns, "
            f"{len(self.deny_patterns)} deny patterns, "
            f"{len(self._robots_cache)} robots.txt entries"
        )
    
    # =========================================================================
    # Robots.txt Cache Management
    # =========================================================================
    
    def _load_robots_cache(self):
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
                            self._robots_cache[domain] = entry
                    except Exception as e:
                        logger.error(f"Failed to parse robots cache line: {e}")
            
            logger.info(f"Loaded {len(self._robots_cache)} robots.txt entries")
            
        except Exception as e:
            logger.error(f"Failed to load robots cache: {e}")
            self._robots_cache = {}
    
    def _persist_robots_entry(self, domain: str, entry: Dict):
        """Persist a single robots cache entry to disk (append).
        
        Args:
            domain: Domain name
            entry: Cache entry to persist
        """
        try:
            with open(self.cache_path, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception as e:
            logger.error(f"Failed to persist robots cache entry for {domain}: {e}")
    
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
    
    async def can_fetch(self, url: str) -> bool:
        """Check if URL can be fetched according to robots.txt.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL can be fetched, False if disallowed
        """
        parts = urlsplit(url)
        domain = parts.netloc.lower()
        
        # Check if we have cached entry
        now = time.time()
        cached = self._robots_cache.get(domain)
        
        if cached:
            # Check if cache is still valid
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
        self._robots_cache[domain] = entry
        self._persist_robots_entry(domain, entry)
        
        # Check permissions
        return self._check_robots_content(content, url)
    
    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt (alias for can_fetch).
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is allowed, False if disallowed
        """
        return await self.can_fetch(url)
    
    # =========================================================================
    # URL Classification
    # =========================================================================
    
    def classify(self, url: str) -> Optional[str]:
        """Classify the page type of a URL.
        
        Args:
            url: URL to classify
            
        Returns:
            Page type string or None if not classifiable
        """
        # Ensure we're working with canonical URL
        url = canonicalize(url)
        
        # Topics page
        if re.match(r"^https://github\.com/topics(?:/[^/?#]+)?$", url):
            return "topic"
        
        # Trending page (exact match, no params)
        if url == "https://github.com/trending":
            return "trending"
        
        # Repository root
        if re.match(r"^https://github\.com/[^/]+/[^/]+/?$", url):
            return "repo_root"
        
        # Blob viewer (file content)
        if re.match(r"^https://github\.com/[^/]+/[^/]+/blob/[^?#]+$", url):
            return "blob"
        
        # Issues list or specific issue
        if re.match(r"^https://github\.com/[^/]+/[^/]+/issues/?(\d+)?$", url):
            return "issues"
        
        # Pull requests list or specific PR
        if re.match(r"^https://github\.com/[^/]+/[^/]+/pull/?(\d+)?$", url):
            return "pull"
        
        # Not a classified type
        return None
    
    # =========================================================================
    # Policy Enforcement (Main Gate)
    # =========================================================================
    
    async def gate(self, url: str) -> Dict[str, Any]:
        """Main policy gate - determines if URL can be crawled.
        
        Checks in order:
        1. Valid URL structure
        2. Correct host (github.com only)
        3. Not in denied subdomains
        4. No denied patterns match
        5. At least one allow pattern matches
        6. Robots.txt allows
        
        Args:
            url: URL to check
            
        Returns:
            Dictionary with:
                - ok: bool (True if allowed)
                - reason: str (explanation)
                - page_type: Optional[str] (classification if allowed)
                - canonical_url: str (canonical form)
        """
        # Canonicalize first
        canonical = canonicalize(url)
        
        # Check valid URL structure
        try:
            parts = urlsplit(canonical)
            if not parts.scheme or not parts.netloc:
                return {
                    "ok": False,
                    "reason": "invalid_url_structure",
                    "page_type": None,
                    "canonical_url": canonical
                }
        except Exception:
            return {
                "ok": False,
                "reason": "url_parse_error",
                "page_type": None,
                "canonical_url": url
            }
        
        # Check host is allowed
        host = parts.netloc.lower()
        if host not in self.allowed_hosts:
            return {
                "ok": False,
                "reason": "host_not_allowed",
                "page_type": None,
                "canonical_url": canonical
            }
        
        # Check not in denied subdomains
        if host in self.denied_subdomains:
            return {
                "ok": False,
                "reason": "subdomain_denied",
                "page_type": None,
                "canonical_url": canonical
            }
        
        # Check deny patterns (these override allow patterns)
        for pattern in self.deny_patterns:
            if pattern.search(canonical):
                return {
                    "ok": False,
                    "reason": "denied_pattern",
                    "page_type": None,
                    "canonical_url": canonical
                }
        
        # Check allow patterns (deny-by-default)
        matched = False
        for pattern in self.allow_patterns:
            if pattern.match(canonical):
                matched = True
                break
        
        if not matched:
            return {
                "ok": False,
                "reason": "not_in_allowlist",
                "page_type": None,
                "canonical_url": canonical
            }
        
        # Check robots.txt
        robots_allowed = await self.can_fetch(canonical)
        if not robots_allowed:
            return {
                "ok": False,
                "reason": "robots_disallow",
                "page_type": None,
                "canonical_url": canonical
            }
        
        # URL is allowed - classify it
        page_type = self.classify(canonical)
        
        return {
            "ok": True,
            "reason": "ok",
            "page_type": page_type,
            "canonical_url": canonical
        }
    
    async def check_batch(self, urls: List[str]) -> List[Dict[str, Any]]:
        """Check multiple URLs in parallel.
        
        Args:
            urls: List of URLs to check
            
        Returns:
            List of gate results in same order as input
        """
        import asyncio
        tasks = [self.gate(url) for url in urls]
        return await asyncio.gather(*tasks)
    
    # =========================================================================
    # Statistics and Persistence
    # =========================================================================
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get robots.txt cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        return {
            "cache_entries": len(self._robots_cache),
            "cache_path": str(self.cache_path),
            "cache_ttl_sec": self.cache_ttl_sec,
            "user_agent": self.user_agent
        }
    
    def get_policy_stats(self) -> Dict[str, Any]:
        """Get policy statistics.
        
        Returns:
            Dictionary with policy configuration stats
        """
        return {
            "allowed_hosts": list(self.allowed_hosts),
            "denied_subdomains": list(self.denied_subdomains),
            "allow_patterns_count": len(self.allow_patterns),
            "deny_patterns_count": len(self.deny_patterns),
            "robots_user_agent": self.user_agent,
            "robots_cache_stats": self.get_cache_stats()
        }
    
    def persist(self):
        """Persist entire robots cache to disk (full rewrite).
        
        This is called on shutdown to ensure all entries are saved.
        """
        try:
            with open(self.cache_path, 'w') as f:
                for entry in self._robots_cache.values():
                    f.write(json.dumps(entry) + '\n')
            
            logger.info(f"Persisted robots cache with {len(self._robots_cache)} entries")
            
        except Exception as e:
            logger.error(f"Failed to persist robots cache: {e}")
    
    def close(self):
        """Close policy and persist final state."""
        logger.info("Closing crawl policy...")
        self.persist()
