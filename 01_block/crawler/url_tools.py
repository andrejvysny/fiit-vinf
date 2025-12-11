"""URL utility functions for canonicalization and validation.

Provides tools for normalizing URLs to ensure consistent handling
and deduplication across the crawler.
"""

from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, unquote, quote
from typing import Set, Optional


# Query parameters to preserve (empty set = drop all)
ALLOWED_QUERY_KEYS: Set[str] = set()


def canonicalize(url: str) -> str:
    """Canonicalize a URL for consistent comparison and deduplication.
    
    Steps:
    1. Lower-case scheme and host
    2. Remove fragment
    3. Remove query parameters (unless whitelisted)
    4. Normalize path (remove trailing slash for non-root)
    5. Sort remaining query parameters
    
    Args:
        url: URL to canonicalize
        
    Returns:
        Canonical form of the URL
    """
    try:
        parts = urlsplit(url)
        
        # Lower-case scheme and netloc
        scheme = parts.scheme.lower()
        netloc = parts.netloc.lower()
        
        # Normalize path
        path = parts.path or "/"
        # Remove duplicate slashes
        while "//" in path:
            path = path.replace("//", "/")
        # Remove trailing slash except for root
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        
        # Filter and sort query parameters
        if parts.query and ALLOWED_QUERY_KEYS:
            # Parse query string
            params = parse_qsl(parts.query, keep_blank_values=False)
            # Filter to allowed keys only
            filtered = [(k, v) for k, v in params if k in ALLOWED_QUERY_KEYS]
            # Sort by key for consistency
            query = urlencode(sorted(filtered))
        else:
            # Drop all query parameters
            query = ""
        
        # Always remove fragment
        fragment = ""
        
        return urlunsplit((scheme, netloc, path, query, fragment))
    except Exception:
        # Return original on parse error
        return url


def is_same_host(url: str, host: str) -> bool:
    """Check if URL belongs to the specified host.
    
    Args:
        url: URL to check
        host: Expected host (e.g., "github.com")
        
    Returns:
        True if URL's host matches exactly (case-insensitive)
    """
    try:
        parsed = urlsplit(url)
        return parsed.netloc.lower() == host.lower()
    except Exception:
        return False


def is_github_url(url: str) -> bool:
    """Check if URL is from github.com (not subdomains).
    
    Args:
        url: URL to check
        
    Returns:
        True if URL is from github.com exactly
    """
    return is_same_host(url, "github.com")


def extract_repo_info(url: str) -> Optional[tuple[str, str]]:
    """Extract repository owner and name from GitHub URL.
    
    Args:
        url: GitHub URL
        
    Returns:
        Tuple of (owner, repo) or None if not a repo URL
    """
    try:
        parts = urlsplit(url)
        if not is_same_host(url, "github.com"):
            return None
        
        # Parse path segments
        path = parts.path.strip("/")
        if not path:
            return None
        
        segments = path.split("/")
        if len(segments) >= 2:
            owner, repo = segments[0], segments[1]
            # Validate owner/repo format (basic check)
            if owner and repo and not owner.startswith(".") and not repo.startswith("."):
                return (owner, repo)
        
        return None
    except Exception:
        return None


def normalize_github_url(url: str) -> str:
    """Apply GitHub-specific normalization rules.
    
    Additional rules beyond canonicalize():
    - Remove .git extension from repo URLs
    - Normalize /blob/master to /blob/main (if configured)
    
    Args:
        url: GitHub URL to normalize
        
    Returns:
        Normalized URL
    """
    url = canonicalize(url)
    
    # Remove .git extension if present
    if url.endswith(".git"):
        url = url[:-4]
    
    return url


def is_valid_url(url: str) -> bool:
    """Check if URL has valid structure.
    
    Args:
        url: URL to validate
        
    Returns:
        True if URL can be parsed and has required components
    """
    try:
        parts = urlsplit(url)
        # Must have scheme and netloc at minimum
        return bool(parts.scheme and parts.netloc)
    except Exception:
        return False


def get_url_depth(url: str) -> int:
    """Calculate the depth of a URL based on path segments.
    
    Args:
        url: URL to analyze
        
    Returns:
        Number of path segments (0 for root)
    """
    try:
        parts = urlsplit(url)
        path = parts.path.strip("/")
        if not path:
            return 0
        return len(path.split("/"))
    except Exception:
        return 0
