import html
import logging
import re
from dataclasses import dataclass
from typing import List, Tuple
from urllib.parse import urljoin, urlsplit

logger = logging.getLogger(__name__)

class LinkExtractor:
    
    def extract(self, html_content: str, base_url: str) -> List[str]:
        if not html_content or not base_url:
            return []
        
        try:
            # Regex pattern to match href attributes in <a> tags
            # Handles both quoted (single/double) and unquoted href values
            pattern = r"""<a\s+[^>]*?href\s*=\s*(?:["']([^"']+)["']|([^\s>]+))"""
            
            seen = set()
            results = []
            
            for match in re.finditer(pattern, html_content, re.IGNORECASE | re.DOTALL):
                # Extract href value from either capture group (quoted or unquoted)
                href = match.group(1) or match.group(2)
                
                # Unescape HTML entities (e.g., &amp; -> &)
                href = html.unescape(href).strip()
                
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
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
        try:
            parts = urlsplit(url)
            from urllib.parse import urlunsplit
            return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
        except Exception:
            return url
