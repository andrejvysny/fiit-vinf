"""HTML link extractor for GitHub pages.

Parses HTML content and extracts GitHub URLs from anchor tags.
Handles relative URLs, deduplicates, and filters non-GitHub links.
"""

import logging
from typing import List, Set
from urllib.parse import urljoin, urlsplit
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class LinkExtractor:
    """Extract and normalize links from HTML pages."""
    
    def __init__(self):
        """Initialize link extractor."""
        pass
    
    def extract(self, html: str, base_url: str) -> List[str]:
        """Extract all GitHub URLs from HTML content.
        
        Args:
            html: HTML content as string
            base_url: Base URL for resolving relative links
            
        Returns:
            List of absolute GitHub URLs (deduplicated)
        """
        if not html or not base_url:
            return []
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            links = set()
            
            # Find all anchor tags with href
            for anchor in soup.find_all("a", href=True):
                href = anchor["href"]
                
                # Skip empty, anchor-only, or javascript links
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                
                # Skip mailto and tel links
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                
                # Resolve relative URLs
                absolute_url = urljoin(base_url, href)
                
                # Check if it's a GitHub URL
                if self._is_github_url(absolute_url):
                    # Remove fragment
                    url_without_fragment = self._remove_fragment(absolute_url)
                    links.add(url_without_fragment)
            
            result = list(links)
            logger.debug(f"Extracted {len(result)} links from {base_url}")
            return result
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    def _is_github_url(self, url: str) -> bool:
        """Check if URL is from github.com.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is from github.com (not subdomains)
        """
        try:
            parts = urlsplit(url)
            # Only accept github.com, not subdomains
            return parts.netloc.lower() == "github.com" and parts.scheme in ("http", "https")
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
    
    def extract_repo_links(self, html: str, base_url: str, owner: str, repo: str) -> List[str]:
        """Extract links specific to a repository.
        
        Only returns links that belong to the same repository.
        
        Args:
            html: HTML content
            base_url: Base URL
            owner: Repository owner
            repo: Repository name
            
        Returns:
            List of URLs within the same repository
        """
        all_links = self.extract(html, base_url)
        
        # Filter to same repo
        repo_prefix = f"https://github.com/{owner}/{repo}/"
        repo_root = f"https://github.com/{owner}/{repo}"
        
        repo_links = [
            link for link in all_links
            if link.startswith(repo_prefix) or link == repo_root
        ]
        
        return repo_links
    
    def extract_topic_links(self, html: str, base_url: str) -> List[str]:
        """Extract topic-related links.
        
        Filters for /topics/* URLs only.
        
        Args:
            html: HTML content
            base_url: Base URL
            
        Returns:
            List of topic URLs
        """
        all_links = self.extract(html, base_url)
        
        # Filter for topics
        topic_links = [
            link for link in all_links
            if "/topics/" in link or link == "https://github.com/topics"
        ]
        
        return topic_links
    
    def extract_trending_links(self, html: str, base_url: str) -> List[str]:
        """Extract repository links from trending page.
        
        Args:
            html: HTML content
            base_url: Base URL
            
        Returns:
            List of repository root URLs found on trending page
        """
        all_links = self.extract(html, base_url)
        
        # Filter for repo root URLs (owner/repo pattern)
        import re
        repo_pattern = re.compile(r"^https://github\.com/[^/]+/[^/]+/?$")
        
        repo_links = [
            link for link in all_links
            if repo_pattern.match(link)
        ]
        
        return repo_links
