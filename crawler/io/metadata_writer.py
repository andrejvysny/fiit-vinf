"""Unified metadata writer for crawl+scrape data.

Writes single JSONL file with all metadata (no rotation).
Combines crawler discovery data with scraper fetch/storage metadata.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class UnifiedMetadataWriter:
    """Writes unified crawl+scrape metadata to single JSONL file."""
    
    def __init__(self, metadata_path: str):
        """Initialize metadata writer.
        
        Args:
            metadata_path: Path to crawl_metadata.jsonl file
        """
        self.metadata_path = Path(metadata_path)
        self.metadata_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open file for append-only writes
        self._file_handle = None
        self._open_for_append()
        
        # Statistics
        self.records_written = 0
        
        logger.info(f"Metadata writer initialized: {self.metadata_path}")
    
    def _open_for_append(self):
        """Open file for append-only writes."""
        try:
            self._file_handle = open(self.metadata_path, 'a', buffering=1)  # Line buffered
            logger.info("Metadata file opened for writing")
        except Exception as e:
            logger.error(f"Failed to open metadata file: {e}")
            self._file_handle = None
    
    def write(self, record: Dict[str, Any]):
        """Write unified metadata record.
        
        Args:
            record: Metadata record containing:
                - url: str
                - timestamp: int
                - depth: int
                - page_type: str
                - referrer: Optional[str]
                - http_status: int
                - content_type: str
                - encoding: str
                - content_sha256: str
                - content_bytes: int
                - stored_path: str
                - etag: str
                - last_modified: str
                - fetch_latency_ms: float
                - retries: int
                - proxy_id: Optional[str]
                - metadata: Dict (additional metadata)
        """
        if not self._file_handle:
            logger.error("Metadata file not open - cannot write record")
            return
        
        # Ensure timestamp is set
        if 'timestamp' not in record:
            record['timestamp'] = int(time.time())
        
        try:
            # Write JSONL line
            self._file_handle.write(json.dumps(record) + '\n')
            self._file_handle.flush()
            self.records_written += 1
            
        except Exception as e:
            logger.error(f"Failed to write metadata record: {e}")
    
    def build_record(
        self,
        url: str,
        depth: int,
        page_type: str,
        referrer: Optional[str],
        http_status: int,
        content_sha256: str,
        stored_path: str,
        content_bytes: int = 0,
        content_type: str = "",
        encoding: str = "",
        etag: str = "",
        last_modified: str = "",
        fetch_latency_ms: float = 0.0,
        retries: int = 0,
        proxy_id: Optional[str] = None,
        extra_metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Build unified metadata record.
        
        Args:
            url: URL fetched
            depth: Crawl depth
            page_type: Page type classification
            referrer: Referrer URL (if any)
            http_status: HTTP status code
            content_sha256: SHA-256 hash of content
            stored_path: Path where HTML is stored
            content_bytes: Size in bytes
            content_type: Content-Type header
            encoding: Content-Encoding header
            etag: ETag header
            last_modified: Last-Modified header
            fetch_latency_ms: Fetch latency in milliseconds
            retries: Number of retries
            proxy_id: Proxy ID used (if any)
            extra_metadata: Additional metadata dict
            
        Returns:
            Metadata record dict
        """
        record = {
            'url': url,
            'timestamp': int(time.time()),
            'depth': depth,
            'page_type': page_type,
            'referrer': referrer,
            'http_status': http_status,
            'content_type': content_type,
            'encoding': encoding,
            'content_sha256': content_sha256,
            'content_bytes': content_bytes,
            'stored_path': stored_path,
            'etag': etag,
            'last_modified': last_modified,
            'fetch_latency_ms': fetch_latency_ms,
            'retries': retries,
            'proxy_id': proxy_id,
            'metadata': extra_metadata or {}
        }
        
        return record
    
    def close(self):
        """Close metadata file."""
        if self._file_handle:
            try:
                self._file_handle.close()
                logger.info(f"Metadata writer closed - total records: {self.records_written}")
            except Exception as e:
                logger.error(f"Error closing metadata file: {e}")
