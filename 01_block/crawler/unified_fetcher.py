import httpx
import asyncio
import time
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from urllib.parse import urlsplit

from .config import ProxyConfig

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

logger = logging.getLogger(__name__)


class UnifiedFetcher:
    
    def __init__(
        self,
        store_root: str,
        user_agent: str,
        user_agents: Optional[list] = None,
        accept_language: str = "en",
        accept_encoding: str = "br, gzip",
        connect_timeout_ms: int = 4000,
        read_timeout_ms: int = 15000,
        max_retries: int = 3,
        backoff_base_ms: int = 500,
        backoff_cap_ms: int = 8000,
        req_per_sec: float = 1.0,
        compress: bool = False,
        user_agent_rotation_size: int = 1,
        permissions: int = 0o644,
        proxy_config: Optional[ProxyConfig] = None
    ):

        self.store_root = Path(store_root)
        self.store_root.mkdir(parents=True, exist_ok=True)
        
        # User-Agent handling: rotate through provided list if available
        self.user_agents = list(user_agents) if user_agents else [user_agent]
        if not self.user_agents:
            self.user_agents = [user_agent]
        self._ua_index = 0
        self._ua_counter = 0
        self.accept_language = accept_language
        self.accept_encoding = accept_encoding
        self.max_retries = max_retries
        self.backoff_base_ms = backoff_base_ms
        self.backoff_cap_ms = backoff_cap_ms
        self.req_per_sec = req_per_sec
        self.compress = compress
        self.permissions = permissions
        self.user_agent_rotation_size = max(1, int(user_agent_rotation_size or 1))
        self.proxy_config = proxy_config or ProxyConfig()
        self._proxy_pool = list(self.proxy_config.pool)
        self._proxy_index = 0
        if self.proxy_config.enabled:
            logger.info("Proxy configuration detected; requests remain direct until proxy routing is enabled")
        
        if compress and not ZSTD_AVAILABLE:
            logger.warning("zstd compression requested but zstandard package not available")
            self.compress = False
        
        self.client = self._create_http_client(
            connect_timeout_ms=connect_timeout_ms,
            read_timeout_ms=read_timeout_ms
        )
        
        self._last_request_time = 0.0
        self._rate_lock = asyncio.Lock()
        
        self.total_fetches = 0
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.files_stored = 0
        self.bytes_stored = 0
        self.bytes_downloaded = 0
        
        logger.info(f"Unified fetcher initialized - store: {self.store_root}, compress: {self.compress}")
    
    def _create_http_client(self, connect_timeout_ms: int, read_timeout_ms: int) -> httpx.AsyncClient:
        """Construct the HTTP client; proxy attachment will be added later."""
        timeout = httpx.Timeout(
            connect=connect_timeout_ms / 1000,
            read=read_timeout_ms / 1000,
            write=read_timeout_ms / 1000,
            pool=None
        )

        client = httpx.AsyncClient(
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

        return client

    def _select_proxy(self) -> Optional[str]:
        if not self.proxy_config.enabled:
            return None

        if self._proxy_pool:
            choice = self._proxy_pool[self._proxy_index % len(self._proxy_pool)]
            self._proxy_index += 1
            return choice

        return self.proxy_config.http_url or self.proxy_config.https_url

    async def _wait_for_rate_limit(self):
        async with self._rate_lock:
            now = time.time()
            min_interval = 1.0 / self.req_per_sec
            elapsed = now - self._last_request_time
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)
            
            self._last_request_time = time.time()
    
    async def fetch_and_store(self, url: str) -> Dict[str, Any]:
        self.total_fetches += 1
        
        # Wait for rate limit
        await self._wait_for_rate_limit()
        
        retries = 0
        
        while retries <= self.max_retries:
            headers: Dict[str, str] = {}
            ua: Optional[str] = None
            try:
                ua = self.user_agents[self._ua_index % len(self.user_agents)]
                headers = {
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": self.accept_language,
                    "Accept-Encoding": self.accept_encoding,
                    "Connection": "keep-alive",
                }
                logger.info("Request headers for %s: %s", url, headers)
                self._advance_user_agent()
                
                start_time = time.time()
                
                response = await self.client.get(url, headers=headers)
                latency_ms = (time.time() - start_time) * 1000
                
                if response.status_code == 200:
                    html_bytes = response.content
                    self.bytes_downloaded += len(html_bytes)
                    
                    content_type = response.headers.get("content-type", "")
                    encoding = response.headers.get("content-encoding", "")
                    etag = response.headers.get("etag", "")
                    last_modified = response.headers.get("last-modified", "")
                    
                    if "text/html" not in content_type.lower() and "text/html" not in content_type:
                        # Not HTML, but we'll store it anyway for this implementation
                        logger.debug(f"Non-HTML content type: {content_type} for {url}")
                    
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
                        'user_agent': ua,
                        'request_headers': headers,
                        'html': html_bytes.decode('utf-8', errors='ignore')  # Return HTML for link extraction
                    }
                
                elif response.status_code in [404, 403, 410]:
                    self.failed_fetches += 1
                    return {
                        'ok': False,
                        'status': response.status_code,
                        'error': f"HTTP {response.status_code}",
                        'fetch_latency_ms': latency_ms,
                        'retries': retries,
                        'user_agent': ua,
                        'request_headers': headers
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
                    logger.warning(f"HTTP {response.status_code}: {url}, attempt {retries + 1}/{self.max_retries + 1}")
                    
                    backoff_ms = min(self.backoff_base_ms * (2 ** retries), self.backoff_cap_ms)
                    await asyncio.sleep(backoff_ms / 1000.0)
                    
                    retries += 1
                    continue
            
            except httpx.RequestError as e:
                logger.warning(f"Request error for {url}: {e}, attempt {retries + 1}/{self.max_retries + 1}")
                
                if retries >= self.max_retries:
                    self.failed_fetches += 1
                    return {
                        'ok': False,
                        'status': 0,
                        'error': str(e),
                        'retries': retries,
                        'user_agent': ua,
                        'request_headers': headers
                    }
                
                backoff_ms = min(self.backoff_base_ms * (2 ** retries), self.backoff_cap_ms)
                await asyncio.sleep(backoff_ms / 1000.0)
                
                retries += 1
                continue
            
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {e}")
                self.failed_fetches += 1
                return {
                    'ok': False,
                    'status': 0,
                    'error': str(e),
                    'retries': retries,
                    'user_agent': ua,
                    'request_headers': headers
                }
        
        self.failed_fetches += 1
        return {
            'ok': False,
            'status': 0,
            'error': 'Max retries exceeded',
            'retries': retries,
            'user_agent': ua,
            'request_headers': headers
        }
    
    def _advance_user_agent(self) -> None:
        self._ua_counter += 1
        if self._ua_counter >= self.user_agent_rotation_size:
            self._ua_counter = 0
            if len(self.user_agents) > 1:
                self._ua_index = (self._ua_index + 1) % len(self.user_agents)

    def _store_html(self, html_bytes: bytes) -> Dict[str, Any]:
        sha256 = hashlib.sha256(html_bytes).hexdigest()
        
        subdir = self.store_root / sha256[:2] / sha256[2:4]
        subdir.mkdir(parents=True, exist_ok=True)
        
        if self.compress:
            file_path = subdir / f"{sha256}.html.zst"
        else:
            file_path = subdir / f"{sha256}.html"
        
        if file_path.exists():
            return {
                'sha256': sha256,
                'stored_path': str(file_path),
                'already_exists': True
            }
        
        if self.compress:
            compressor = zstd.ZstdCompressor(level=10, threads=-1)
            data_to_write = compressor.compress(html_bytes)
        else:
            data_to_write = html_bytes
        
        # Write atomically (via temp file)
        tmp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        
        try:
            with open(tmp_path, 'wb') as f:
                f.write(data_to_write)
            
            tmp_path.chmod(self.permissions)
            
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
            if tmp_path.exists():
                tmp_path.unlink()
            
            return {
                'sha256': sha256,
                'stored_path': '',
                'error': str(e)
            }
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
        logger.info(f"Unified fetcher closed - fetches: {self.total_fetches}, successes: {self.successful_fetches}, files stored: {self.files_stored}")
