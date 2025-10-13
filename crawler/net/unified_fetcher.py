"""Unified fetcher that combines HTTP fetching with HTML storage.

Single fetch operation that:
1. Fetches HTML from URL
2. Stores HTML in content-addressed store
3. Returns unified metadata

Eliminates duplicate fetches between crawler and scraper.
"""

import httpx
import asyncio
import time
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urlsplit

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

logger = logging.getLogger(__name__)


class UnifiedFetcher:
    """Unified HTTP fetcher with integrated HTML storage."""
    
    def __init__(
        self,
        store_root: str,
        user_agent: str,
        accept_language: str = "en",
        accept_encoding: str = "br, gzip",
        connect_timeout_ms: int = 4000,
        read_timeout_ms: int = 15000,
        max_retries: int = 3,
        backoff_base_ms: int = 500,
        backoff_cap_ms: int = 8000,
        req_per_sec: float = 1.0,
        compress: bool = False,
        permissions: int = 0o644
    ):
        """Initialize unified fetcher.
        
        Args:
            store_root: Root directory for HTML storage
            user_agent: User agent string
            accept_language: Accept-Language header
            accept_encoding: Accept-Encoding header
            connect_timeout_ms: Connection timeout in milliseconds
            read_timeout_ms: Read timeout in milliseconds
            max_retries: Maximum retry attempts
            backoff_base_ms: Base backoff in milliseconds
            backoff_cap_ms: Maximum backoff in milliseconds
            req_per_sec: Request rate limit (requests per second)
            compress: Enable zstd compression for storage
            permissions: File permissions for stored HTML
        """
        self.store_root = Path(store_root)
        self.store_root.mkdir(parents=True, exist_ok=True)
        
        self.user_agent = user_agent
        self.accept_language = accept_language
        self.accept_encoding = accept_encoding
        self.max_retries = max_retries
        self.backoff_base_ms = backoff_base_ms
        self.backoff_cap_ms = backoff_cap_ms
        self.req_per_sec = req_per_sec
        self.compress = compress
        self.permissions = permissions
        
        if compress and not ZSTD_AVAILABLE:
            logger.warning("zstd compression requested but zstandard package not available")
            self.compress = False
        
        # Create HTTP client
        timeout = httpx.Timeout(
            connect=connect_timeout_ms / 1000,
            read=read_timeout_ms / 1000,
            write=read_timeout_ms / 1000,
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
        
        # Rate limiting
        self._last_request_time = 0.0
        self._rate_lock = asyncio.Lock()
        
        # Statistics
        self.total_fetches = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.files_stored = 0
        self.bytes_stored = 0
        self.bytes_downloaded = 0
        
        logger.info(f"Unified fetcher initialized - store: {self.store_root}, compress: {self.compress}")
    
    async def _wait_for_rate_limit(self):
        """Enforce rate limit by waiting if necessary."""
        async with self._rate_lock:
            now = time.time()
            min_interval = 1.0 / self.req_per_sec
            elapsed = now - self._last_request_time
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)
            
            self._last_request_time = time.time()
    
    async def fetch_and_store(self, url: str) -> Dict[str, Any]:
        """Fetch URL and store HTML in one operation.
        
        Args:
            url: URL to fetch
            
        Returns:
            Result dict with:
            - ok: bool (success flag)
            - status: int (HTTP status code)
            - error: str (error message if failed)
            - content_sha256: str (SHA-256 hash)
            - stored_path: str (path to stored file)
            - content_bytes: int (original size)
            - content_type: str
            - encoding: str
            - etag: str
            - last_modified: str
            - fetch_latency_ms: float
            - retries: int
        """
        self.total_fetches += 1
        
        # Wait for rate limit
        await self._wait_for_rate_limit()
        
        retries = 0
        
        while retries <= self.max_retries:
            try:
                # Build headers
                headers = {
                    "User-Agent": self.user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": self.accept_language,
                    "Accept-Encoding": self.accept_encoding,
                    "Connection": "keep-alive",
                }
                
                # Fetch
                start_time = time.time()
                
                response = await self.client.get(url, headers=headers)
                latency_ms = (time.time() - start_time) * 1000
                
                # Check status
                if response.status_code == 200:
                    # Success
                    html_bytes = response.content
                    self.bytes_downloaded += len(html_bytes)
                    
                    # Extract headers
                    content_type = response.headers.get("content-type", "")
                    encoding = response.headers.get("content-encoding", "")
                    etag = response.headers.get("etag", "")
                    last_modified = response.headers.get("last-modified", "")
                    
                    # Check if HTML (simple check)
                    if "text/html" not in content_type.lower() and "text/html" not in content_type:
                        # Not HTML, but we'll store it anyway for this implementation
                        logger.debug(f"Non-HTML content type: {content_type} for {url}")
                    
                    # Store HTML
                    storage_result = self._store_html(html_bytes)
                    
                    self.successful_fetches += 1
                    
                    return {
                        'ok': True,
                        'status': response.status_code,
                        'content_sha256': storage_result['sha256'],
                        'stored_path': storage_result['stored_path'],
                        'content_bytes': len(html_bytes),
                        'content_type': content_type,
                        'encoding': encoding,
                        'etag': etag,
                        'last_modified': last_modified,
                        'fetch_latency_ms': latency_ms,
                        'retries': retries,
                        'html': html_bytes.decode('utf-8', errors='ignore')  # Return HTML for link extraction
                    }
                
                elif response.status_code in [404, 403, 410]:
                    # Client error - don't retry
                    self.failed_fetches += 1
                    return {
                        'ok': False,
                        'status': response.status_code,
                        'error': f"HTTP {response.status_code}",
                        'fetch_latency_ms': latency_ms,
                        'retries': retries
                    }
                
                elif response.status_code == 429:
                    # Rate limited - retry with backoff
                    logger.warning(f"429 Rate Limited: {url}, attempt {retries + 1}/{self.max_retries + 1}")
                    
                    # Check for Retry-After header
                    retry_after = response.headers.get("retry-after")
                    if retry_after:
                        try:
                            wait_sec = int(retry_after)
                            logger.info(f"Retry-After: {wait_sec}s")
                            await asyncio.sleep(min(wait_sec, 60))
                        except:
                            pass
                    
                    # Exponential backoff
                    backoff_ms = min(self.backoff_base_ms * (2 ** retries), self.backoff_cap_ms)
                    await asyncio.sleep(backoff_ms / 1000.0)
                    
                    retries += 1
                    continue
                
                else:
                    # Server error - retry with backoff
                    logger.warning(f"HTTP {response.status_code}: {url}, attempt {retries + 1}/{self.max_retries + 1}")
                    
                    backoff_ms = min(self.backoff_base_ms * (2 ** retries), self.backoff_cap_ms)
                    await asyncio.sleep(backoff_ms / 1000.0)
                    
                    retries += 1
                    continue
            
            except httpx.RequestError as e:
                # Network error - retry
                logger.warning(f"Request error for {url}: {e}, attempt {retries + 1}/{self.max_retries + 1}")
                
                if retries >= self.max_retries:
                    self.failed_fetches += 1
                    return {
                        'ok': False,
                        'status': 0,
                        'error': str(e),
                        'retries': retries
                    }
                
                backoff_ms = min(self.backoff_base_ms * (2 ** retries), self.backoff_cap_ms)
                await asyncio.sleep(backoff_ms / 1000.0)
                
                retries += 1
                continue
            
            except Exception as e:
                # Unexpected error
                logger.error(f"Unexpected error fetching {url}: {e}")
                self.failed_fetches += 1
                return {
                    'ok': False,
                    'status': 0,
                    'error': str(e),
                    'retries': retries
                }
        
        # Max retries exceeded
        self.failed_fetches += 1
        return {
            'ok': False,
            'status': 0,
            'error': 'Max retries exceeded',
            'retries': retries
        }
    
    def _store_html(self, html_bytes: bytes) -> Dict[str, Any]:
        """Store HTML content to disk (content-addressed).
        
        Args:
            html_bytes: Raw HTML bytes
            
        Returns:
            Dict with:
            - sha256: Content hash
            - stored_path: Path where file was stored
            - already_exists: bool
        """
        # Calculate SHA-256 hash
        sha256 = hashlib.sha256(html_bytes).hexdigest()
        
        # Create nested directory structure (aa/bb/sha256.html[.zst])
        subdir = self.store_root / sha256[:2] / sha256[2:4]
        subdir.mkdir(parents=True, exist_ok=True)
        
        # Determine file path
        if self.compress:
            file_path = subdir / f"{sha256}.html.zst"
        else:
            file_path = subdir / f"{sha256}.html"
        
        # Check if already exists (deduplication)
        if file_path.exists():
            return {
                'sha256': sha256,
                'stored_path': str(file_path),
                'already_exists': True
            }
        
        # Prepare data for writing
        if self.compress:
            compressor = zstd.ZstdCompressor(level=10, threads=-1)
            data_to_write = compressor.compress(html_bytes)
        else:
            data_to_write = html_bytes
        
        # Write atomically (via temp file)
        tmp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        
        try:
            # Write to temp file
            with open(tmp_path, 'wb') as f:
                f.write(data_to_write)
            
            # Set permissions
            tmp_path.chmod(self.permissions)
            
            # Atomic rename
            tmp_path.replace(file_path)
            
            self.files_stored += 1
            self.bytes_stored += len(html_bytes)
            
            return {
                'sha256': sha256,
                'stored_path': str(file_path),
                'already_exists': False
            }
            
        except Exception as e:
            logger.error(f"Failed to store HTML for sha256={sha256}: {e}")
            # Clean up temp file
            if tmp_path.exists():
                tmp_path.unlink()
            
            # Return error but include sha256
            return {
                'sha256': sha256,
                'stored_path': '',
                'error': str(e)
            }
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
        logger.info(f"Unified fetcher closed - fetches: {self.total_fetches}, successes: {self.successful_fetches}, files stored: {self.files_stored}")
