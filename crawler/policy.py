"""URL policy enforcement for the GitHub crawler.

Implements strict deny-by-default policy with allowlist patterns
and robots.txt compliance checking.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlsplit

from .url_tools import canonicalize, is_github_url
from .robots_cache import RobotsCache
from .config import CrawlerConfig

logger = logging.getLogger(__name__)


class UrlPolicy:
    """Enforces URL crawling policy with strict deny-by-default."""
    
    def __init__(self, config: CrawlerConfig, robots: RobotsCache):
        """Initialize URL policy.
        
        Args:
            config: Crawler configuration
            robots: Robots.txt cache instance
        """
        self.config = config
        self.robots = robots
        
        # Compile regex patterns for efficiency
        self.allow_patterns = [
            re.compile(pattern) for pattern in config.scope.allow_patterns
        ]
        self.deny_patterns = [
            re.compile(pattern) for pattern in config.scope.deny_patterns
        ]
        
        # Convert host lists to sets for O(1) lookup
        self.allowed_hosts = set(config.scope.allowed_hosts)
        self.denied_subdomains = set(config.scope.denied_subdomains)
        
        logger.info(f"Policy initialized with {len(self.allow_patterns)} allow patterns, "
                   f"{len(self.deny_patterns)} deny patterns")
    
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
    
    async def gate(self, url: str) -> Dict[str, any]:
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
        robots_allowed = await self.robots.is_allowed(canonical)
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
    
    async def check_batch(self, urls: List[str]) -> List[Dict[str, any]]:
        """Check multiple URLs in parallel.
        
        Args:
            urls: List of URLs to check
            
        Returns:
            List of gate results in same order as input
        """
        import asyncio
        tasks = [self.gate(url) for url in urls]
        return await asyncio.gather(*tasks)
    
    def get_policy_stats(self) -> Dict[str, any]:
        """Get policy statistics.
        
        Returns:
            Dictionary with policy configuration stats
        """
        return {
            "allowed_hosts": list(self.allowed_hosts),
            "denied_subdomains": list(self.denied_subdomains),
            "allow_patterns_count": len(self.allow_patterns),
            "deny_patterns_count": len(self.deny_patterns),
            "robots_user_agent": self.robots.user_agent,
            "robots_cache_stats": self.robots.get_cache_stats()
        }
