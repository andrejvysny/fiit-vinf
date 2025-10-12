"""
Proxy client interface for Manager integration
"""

from typing import Optional, Dict, Any
from abc import ABC, abstractmethod


class ProxyClient(ABC):
    """
    Abstract proxy client interface.
    Manager will provide concrete implementation.
    """

    @abstractmethod
    async def pick(self) -> Optional[Dict[str, Any]]:
        """
        Select a proxy for the next request.

        Returns:
            Proxy descriptor dict or None:
            {
                "proxy_id": "p1",
                "http": "http://user:pass@host:port",
                "https": "http://user:pass@host:port"
            }
        """
        pass

    @abstractmethod
    async def report_result(self, proxy_id: Optional[str], status: Optional[int],
                           throttled: bool) -> None:
        """
        Report proxy result back to Manager.

        Args:
            proxy_id: ID of proxy used (None if no proxy)
            status: HTTP status code (None on network error)
            throttled: True if 429 or Retry-After header present
        """
        pass


class NoProxyClient(ProxyClient):
    """
    Null proxy client for standalone operation (no proxies).
    """

    async def pick(self) -> Optional[Dict[str, Any]]:
        """No proxy available"""
        return None

    async def report_result(self, proxy_id: Optional[str], status: Optional[int],
                           throttled: bool) -> None:
        """Nothing to report"""
        pass