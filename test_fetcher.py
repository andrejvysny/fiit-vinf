#!/usr/bin/env python3
"""
Test script for Fetcher - dry run with real GitHub URLs
"""

import asyncio
from scraper.config import ScraperConfig
from scraper.net.fetcher import Fetcher
from scraper.net.proxy_client import NoProxyClient
from scraper.util.limits import RateLimiter


async def test_fetcher():
    """Test fetcher with real GitHub URLs (dry run)"""

    # Create minimal config
    config = ScraperConfig(
        run_id="test-run",
        workspace="/tmp",
        user_agent="TestScraper/1.0 (+test@example.com)"
    )

    # Create components
    proxy_client = NoProxyClient()
    limiter = RateLimiter(global_concurrency=2, req_per_sec=1.0)
    fetcher = Fetcher(config, proxy_client, limiter)

    print("TEST: Fetcher dry run with GitHub URLs")
    print("=" * 60)
    print(f"User-Agent: {config.user_agent}")
    print(f"Concurrency: {config.limits.global_concurrency}")
    print(f"Rate limit: 1 req/sec")
    print()

    # Test URLs
    test_urls = [
        "https://github.com/python/cpython",
        "https://github.com/torvalds/linux",
        "https://github.com/facebook/react"
    ]

    print("Fetching test URLs:")
    print("-" * 40)

    for url in test_urls:
        print(f"\nFetching: {url}")

        # First fetch (no conditional headers)
        result = await fetcher.fetch_html(url)

        if result["ok"]:
            print(f"  ✓ Status: {result['status']}")
            print(f"  ✓ Size: {len(result.get('html_bytes', b''))} bytes")
            print(f"  ✓ Latency: {result['latency_ms']:.0f}ms")

            # Check for ETag/Last-Modified
            headers = result.get("headers", {})
            etag = headers.get("etag") or headers.get("ETag")
            last_modified = headers.get("last-modified") or headers.get("Last-Modified")

            if etag:
                print(f"  ✓ ETag: {etag[:30]}...")
            if last_modified:
                print(f"  ✓ Last-Modified: {last_modified}")

            # Test conditional request
            if etag or last_modified:
                print(f"\n  Testing conditional request...")
                prev = {
                    "etag": etag,
                    "last_modified": last_modified
                }
                result2 = await fetcher.fetch_html(url, prev)

                if result2.get("not_modified"):
                    print(f"  ✓ Got 304 Not Modified (as expected)")
                else:
                    print(f"  ! Got {result2.get('status')} (GitHub may not support conditional requests)")

        else:
            print(f"  ✗ Failed: {result.get('error')}")
            if result.get("not_html"):
                print(f"    Content-Type: {result.get('content_type')}")

    print()
    print("Statistics:")
    print("-" * 40)
    stats = fetcher.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    limiter_stats = limiter.get_stats()
    print(f"\nRate Limiter Stats:")
    print(f"  Total wait time: {limiter_stats['total_wait_time']}")
    print(f"  Average wait: {limiter_stats['avg_wait_time']}")

    # Cleanup
    await fetcher.close()

    print()
    print("✅ Fetcher test complete!")


if __name__ == "__main__":
    asyncio.run(test_fetcher())