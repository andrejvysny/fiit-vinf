#!/usr/bin/env python3
"""Test script for URL policy validation."""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from crawler.config import CrawlerConfig
from crawler.policy import UrlPolicy
from crawler.robots_cache import RobotsCache
from crawler.url_tools import canonicalize


# Test URLs - mix of allowed, denied, and borderline cases
TEST_URLS = [
    # Allowed URLs
    ("https://github.com/topics", "Should be allowed - topics page"),
    ("https://github.com/topics/python", "Should be allowed - specific topic"),
    ("https://github.com/trending", "Should be allowed - trending page"),
    ("https://github.com/python/cpython", "Should be allowed - repo root"),
    ("https://github.com/torvalds/linux/", "Should be allowed - repo root with trailing slash"),
    ("https://github.com/microsoft/vscode/blob/main/README.md", "Should be allowed - blob viewer"),
    ("https://github.com/nodejs/node/issues", "Should be allowed - issues list"),
    ("https://github.com/nodejs/node/issues/1234", "Should be allowed - specific issue"),
    ("https://github.com/facebook/react/pull", "Should be allowed - PR list"),
    ("https://github.com/facebook/react/pull/5678", "Should be allowed - specific PR"),
    
    # Denied URLs
    ("https://api.github.com/repos/python/cpython", "Should be denied - subdomain"),
    ("https://raw.githubusercontent.com/python/cpython/main/README.md", "Should be denied - subdomain"),
    ("https://github.com/search?q=python", "Should be denied - search"),
    ("https://github.com/python/cpython/tree/main", "Should be denied - tree view"),
    ("https://github.com/python/cpython/commits/main", "Should be denied - commits"),
    ("https://github.com/python/cpython/graphs/contributors", "Should be denied - graphs"),
    ("https://github.com/python/cpython/archive/refs/heads/main.zip", "Should be denied - archive"),
    ("https://github.com/python/cpython/stargazers", "Should be denied - stargazers"),
    ("https://github.com/trending?since=weekly", "Should be denied - trending with params"),
    ("https://github.com/python/cpython?tab=readme-ov-file", "Should be denied - tab param"),
    
    # Edge cases
    ("https://github.com", "Edge case - homepage"),
    ("https://github.com/", "Edge case - homepage with slash"),
    ("https://github.com/python", "Edge case - user/org page"),
    ("http://github.com/python/cpython", "Edge case - http instead of https"),
    ("HTTPS://GITHUB.COM/PYTHON/CPYTHON", "Edge case - uppercase"),
    ("https://github.com/python/cpython#readme", "Edge case - with fragment"),
    ("https://gitlab.com/python/cpython", "Should be denied - wrong host"),
    ("not-a-url", "Should be denied - invalid URL"),
]


async def test_policy():
    """Test the policy validation."""
    print("="*70)
    print("Testing URL Policy Validation")
    print("="*70)
    
    # Load config
    config = CrawlerConfig.from_yaml("config.yaml")
    
    # Initialize components
    robots = RobotsCache(config.robots.user_agent, config.robots.cache_ttl_sec)
    policy = UrlPolicy(config, robots)
    
    # Test each URL
    for url, description in TEST_URLS:
        print(f"\n{description}")
        print(f"URL: {url}")
        print(f"Canonical: {canonicalize(url)}")
        
        result = await policy.gate(url)
        
        status = "✅ ALLOWED" if result["ok"] else "❌ DENIED"
        print(f"Result: {status}")
        print(f"Reason: {result['reason']}")
        
        if result.get("page_type"):
            print(f"Page Type: {result['page_type']}")
        
        print("-" * 50)
    
    print("\n" + "="*70)
    print("Test Complete")
    print("="*70)


if __name__ == "__main__":
    asyncio.run(test_policy())
