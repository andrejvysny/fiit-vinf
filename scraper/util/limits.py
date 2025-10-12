"""
Rate limiting and concurrency control
"""

import asyncio
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Dict, Optional


class RateLimiter:
    """
    Rate limiter with global concurrency control and per-host rate limiting.
    """

    def __init__(self, global_concurrency: int = 2, req_per_sec: float = 1.0):
        """
        Initialize rate limiter.

        Args:
            global_concurrency: Maximum concurrent requests globally
            req_per_sec: Maximum requests per second (globally)
        """
        self.global_concurrency = global_concurrency
        self.req_per_sec = req_per_sec

        # Global semaphore for concurrency
        self.global_sem = asyncio.Semaphore(global_concurrency)

        # Token bucket for rate limiting
        self.last_request_time = 0
        self.min_interval = 1.0 / req_per_sec if req_per_sec > 0 else 0

        # Per-host tracking (for future per-host limits)
        self.host_last_request = defaultdict(float)

        # Statistics
        self.total_requests = 0
        self.total_wait_time = 0
        self.active_requests = 0

    @asynccontextmanager
    async def acquire(self, host: str):
        """
        Acquire permission to make a request.

        Args:
            host: Target hostname

        Context manager that handles both concurrency and rate limiting.
        """
        # Wait for concurrency slot
        async with self.global_sem:
            self.active_requests += 1

            # Enforce rate limit
            await self._enforce_rate_limit()

            # Track host request time
            self.host_last_request[host] = time.time()
            self.total_requests += 1

            try:
                yield
            finally:
                self.active_requests -= 1

    async def _enforce_rate_limit(self):
        """Enforce global rate limit using token bucket"""
        if self.min_interval <= 0:
            return

        now = time.time()
        time_since_last = now - self.last_request_time

        if time_since_last < self.min_interval:
            wait_time = self.min_interval - time_since_last
            self.total_wait_time += wait_time
            await asyncio.sleep(wait_time)
            self.last_request_time = time.time()
        else:
            self.last_request_time = now

    def get_stats(self) -> Dict[str, any]:
        """Get rate limiter statistics"""
        return {
            "total_requests": self.total_requests,
            "active_requests": self.active_requests,
            "total_wait_time": f"{self.total_wait_time:.2f}s",
            "avg_wait_time": f"{self.total_wait_time / self.total_requests:.3f}s" if self.total_requests > 0 else "N/A",
            "unique_hosts": len(self.host_last_request),
            "config": {
                "global_concurrency": self.global_concurrency,
                "req_per_sec": self.req_per_sec
            }
        }