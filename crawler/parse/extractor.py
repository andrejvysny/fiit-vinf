"""General-purpose HTML link extractor.

Parses HTML content, extracts URLs from anchor tags, resolves relative links,
removes fragments, and deduplicates results. Domain-level decisions are left to
other components of the crawling pipeline.
"""

import re
import html
import logging
from typing import List
from urllib.parse import urljoin, urlsplit

logger = logging.getLogger(__name__)


class LinkExtractor:
    """Extract and normalize links from HTML pages."""
    
    def __init__(self):
        """Initialize link extractor."""
        pass
    
    def extract(self, html_content: str, base_url: str) -> List[str]:
        """Extract all valid HTTP(S) URLs from HTML content.
        
        Args:
            html_content: HTML content as string
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute URLs (deduplicated, order-preserving)
        """
        if not html_content or not base_url:
            return []
        
        try:
            # Regex pattern to match href attributes in <a> tags
            # Handles both quoted (single/double) and unquoted href values
            pattern = r"""<a\s+[^>]*?href\s*=\s*(?:["']([^"']+)["']|([^\s>]+))"""
            
            seen = set()
            results = []
            
            # Find all href attributes in anchor tags
            for match in re.finditer(pattern, html_content, re.IGNORECASE | re.DOTALL):
                # Extract href value from either capture group (quoted or unquoted)
                href = match.group(1) or match.group(2)
                
                # Unescape HTML entities (e.g., &amp; -> &)
                href = html.unescape(href).strip()
                
                # Skip empty, anchor-only, or javascript links
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                
                # Skip mailto and tel links
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href)
                
                if not self._is_supported_scheme(absolute_url):
                    continue

                url_without_fragment = self._remove_fragment(absolute_url)

                # Track unique URLs while preserving appearance order
                if url_without_fragment not in seen:
                    seen.add(url_without_fragment)
                    results.append(url_without_fragment)
            
            logger.debug(f"Extracted {len(results)} links from {base_url}")
            return results
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    def _is_supported_scheme(self, url: str) -> bool:
        """Check whether URL uses a supported scheme (HTTP or HTTPS)."""
        try:
            parts = urlsplit(url)
            return parts.scheme in ("http", "https") and bool(parts.netloc)
        except Exception:
            return False
    
    def _remove_fragment(self, url: str) -> str:
        """Remove fragment from URL.
        
        Args:
            url: URL to process
            
        Returns:
            URL without fragment
        """
        try:
            parts = urlsplit(url)
            # Reconstruct without fragment
            from urllib.parse import urlunsplit
            return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
        except Exception:
            return url
