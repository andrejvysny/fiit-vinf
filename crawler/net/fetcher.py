"""LEGACY: crawler fetcher removed.

This module belonged to the separated crawler architecture. The unified
single-process fetcher is implemented in `crawler/net/unified_fetcher.py`.
Importing this module will raise an ImportError to avoid accidental usage.
"""

raise ImportError(
    "Legacy CrawlerFetcher removed. Use crawler/net/unified_fetcher.py instead."
)
