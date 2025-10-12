"""
HTTPX-based fetcher with retries, backoff, and conditional requests
"""

import httpx
import asyncio
import time
import random
from typing import Dict, Any, Optional
from urllib.parse import urlsplit

from .proxy_client import ProxyClient
from ..util.limits import RateLimiter


class Fetcher:
    """
    Async HTTP fetcher with retry logic and proxy support.
    """

    def __init__(self, config, proxy_client: ProxyClient, limiter: RateLimiter):
        """
        Initialize fetcher.

        Args:
            config: ScraperConfig instance
            proxy_client: ProxyClient for proxy selection
            limiter: RateLimiter for rate control
        """
        self.config = config
        self.proxy_client = proxy_client
        self.limiter = limiter

        # Create HTTPX client with HTTP/2 support
        timeout = httpx.Timeout(
            connect=config.limits.connect_timeout_ms / 1000,
            read=config.limits.read_timeout_ms / 1000,
            write=config.limits.read_timeout_ms / 1000,
            pool=None
        )

        self.client = httpx.AsyncClient(
            http2=True,
            timeout=timeout,
            limits=httpx.Limits(
                max_keepalive_connections=10,
                max_connections=20,
                keepalive_expiry=30
            ),
            follow_redirects=True,
            max_redirects=5
        )

        # Base headers
        self.base_headers = {
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": config.accept_language,
            "Accept-Encoding": config.accept_encoding,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

        # Statistics
        self.total_fetches = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.not_modified_count = 0
        self.retry_count = 0

    async def fetch_html(self, url: str, prev: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Fetch HTML content with retries and conditional requests.

        Args:
            url: URL to fetch
            prev: Previous page metadata (for conditional requests)

        Returns:
            Result dict with:
            - ok: Success flag
            - status: HTTP status code
            - html_bytes: Raw HTML bytes (if successful)
            - headers: Response headers
            - latency_ms: Request latency in milliseconds
            - proxy_id: ID of proxy used
            - not_modified: True if 304
            - not_html: True if not HTML content
            - retries: Number of retries attempted
            - error: Error message (if failed)
        """
        self.total_fetches += 1
        host = urlsplit(url).netloc.lower()
        retries = 0
        max_retries = self.config.limits.max_retries

        while retries <= max_retries:
            try:
                # Select proxy if available
                proxy = await self.proxy_client.pick() if self.proxy_client else None
                proxy_dict = None
                proxy_id = None

                if proxy:
                    proxy_id = proxy.get("proxy_id")
                    # Format proxy for httpx
                    if "http" in proxy:
                        proxy_dict = {"http://": proxy["http"], "https://": proxy["https"]}

                # Prepare headers
                headers = dict(self.base_headers)

                # Add conditional headers if we have previous data
                if prev:
                    if etag := prev.get("etag"):
                        headers["If-None-Match"] = etag
                    if last_modified := prev.get("last_modified"):
                        headers["If-Modified-Since"] = last_modified

                # Acquire rate limit permission
                async with self.limiter.acquire(host):
                    # Make request
                    start_time = time.time()

                    try:
                        if proxy_dict:
                            response = await self.client.get(
                                url,
                                headers=headers,
                                proxies=proxy_dict
                            )
                        else:
                            response = await self.client.get(
                                url,
                                headers=headers
                            )
                        latency_ms = (time.time() - start_time) * 1000

                    except httpx.RequestError as e:
                        # Network error - retry
                        if self.proxy_client:
                            await self.proxy_client.report_result(proxy_id, None, False)

                        if retries >= max_retries:
                            self.failed_fetches += 1
                            return {
                                "ok": False,
                                "error": str(e),
                                "retries": retries
                            }

                        await self._backoff(retries)
                        retries += 1
                        self.retry_count += 1
                        continue

                # Check for throttling
                throttled = (response.status_code == 429) or ("Retry-After" in response.headers)

                # Report to proxy client
                if self.proxy_client:
                    await self.proxy_client.report_result(
                        proxy_id,
                        response.status_code,
                        throttled
                    )

                # Handle 304 Not Modified
                if response.status_code == 304:
                    self.not_modified_count += 1
                    self.successful_fetches += 1
                    return {
                        "ok": True,
                        "status": 304,
                        "not_modified": True,
                        "headers": dict(response.headers),
                        "latency_ms": latency_ms,
                        "proxy_id": proxy_id,
                        "retries": retries
                    }

                # Handle server errors and rate limiting
                if response.status_code >= 500 or response.status_code in (408, 429):
                    if retries >= max_retries:
                        self.failed_fetches += 1
                        return {
                            "ok": False,
                            "status": response.status_code,
                            "error": f"HTTP {response.status_code}",
                            "retries": retries
                        }

                    # Wait based on Retry-After or exponential backoff
                    await self._retry_after_or_backoff(response, retries)
                    retries += 1
                    self.retry_count += 1
                    continue

                # Check content type
                content_type = response.headers.get("Content-Type", "")
                if "text/html" not in content_type.lower():
                    self.failed_fetches += 1
                    return {
                        "ok": False,
                        "status": response.status_code,
                        "not_html": True,
                        "content_type": content_type,
                        "retries": retries
                    }

                # Success!
                self.successful_fetches += 1
                return {
                    "ok": True,
                    "status": response.status_code,
                    "html_bytes": response.content,  # Raw bytes
                    "headers": dict(response.headers),
                    "latency_ms": latency_ms,
                    "proxy_id": proxy_id,
                    "retries": retries
                }

            except Exception as e:
                # Unexpected error
                if retries >= max_retries:
                    self.failed_fetches += 1
                    return {
                        "ok": False,
                        "error": f"Unexpected error: {e}",
                        "retries": retries
                    }

                await self._backoff(retries)
                retries += 1
                self.retry_count += 1

        # Should not reach here
        self.failed_fetches += 1
        return {
            "ok": False,
            "error": "Max retries exceeded",
            "retries": retries
        }

    async def _retry_after_or_backoff(self, response: httpx.Response, retry_num: int):
        """Handle Retry-After header or use exponential backoff"""
        retry_after = response.headers.get("Retry-After")

        if retry_after:
            try:
                # Could be seconds or HTTP date
                wait_seconds = float(retry_after)
                print(f"Retry-After: waiting {wait_seconds}s")
                await asyncio.sleep(wait_seconds)
                return
            except ValueError:
                # Try parsing as date - for now just use backoff
                pass

        await self._backoff(retry_num)

    async def _backoff(self, retry_num: int):
        """Exponential backoff with jitter"""
        base_ms = self.config.limits.backoff_base_ms
        cap_ms = self.config.limits.backoff_cap_ms

        wait_ms = min(cap_ms, base_ms * (2 ** retry_num))
        # Add jitter (±25%)
        wait_ms = wait_ms * (0.75 + random.random() * 0.5)

        wait_seconds = wait_ms / 1000
        print(f"Backoff: waiting {wait_seconds:.2f}s (retry {retry_num + 1})")
        await asyncio.sleep(wait_seconds)

    def get_stats(self) -> Dict[str, Any]:
        """Get fetcher statistics"""
        return {
            "total_fetches": self.total_fetches,
            "successful": self.successful_fetches,
            "failed": self.failed_fetches,
            "not_modified": self.not_modified_count,
            "total_retries": self.retry_count,
            "success_rate": f"{(self.successful_fetches / self.total_fetches * 100):.1f}%" if self.total_fetches > 0 else "N/A"
        }

    async def close(self):
        """Close HTTP client"""
        await self.client.aclose()