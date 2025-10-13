"""File-based deduplication for tracking fetched URLs.

Replaces LMDB-based dedup with simple file storage.
Loads URLs into memory set for O(1) lookup.
"""

import logging
from pathlib import Path
from typing import Set

logger = logging.getLogger(__name__)


class FileDedupStore:
    """File-based deduplication store for fetched URLs."""
    
    def __init__(self, fetched_urls_path: str):
        """Initialize dedup store.
        
        Args:
            fetched_urls_path: Path to fetched_urls.txt file
        """
        self.fetched_urls_path = Path(fetched_urls_path)
        self.fetched_urls_path.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory set for fast lookup
        self._fetched: Set[str] = set()
        
        # Load existing fetched URLs
        self._load()
        
        # Open file for append-only writes
        self._file_handle = None
        self._open_for_append()
        
        logger.info(f"Dedup store initialized with {len(self._fetched)} fetched URLs")
    
    def _load(self):
        """Load fetched URLs from disk into memory."""
        if not self.fetched_urls_path.exists():
            logger.info("No existing fetched_urls file found - starting fresh")
            # Create empty file
            self.fetched_urls_path.touch()
            return
        
        try:
            with open(self.fetched_urls_path, 'r') as f:
                for line in f:
                    url = line.strip()
                    if url:
                        self._fetched.add(url)
            
            logger.info(f"Loaded {len(self._fetched)} fetched URLs")
            
        except Exception as e:
            logger.error(f"Failed to load fetched URLs: {e}")
            self._fetched = set()
    
    def _open_for_append(self):
        """Open file for append-only writes."""
        try:
            self._file_handle = open(self.fetched_urls_path, 'a', buffering=1)  # Line buffered
        except Exception as e:
            logger.error(f"Failed to open fetched_urls file for append: {e}")
            self._file_handle = None
    
    def is_fetched(self, url: str) -> bool:
        """Check if URL has been fetched.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL was already fetched
        """
        return url in self._fetched
    
    def mark_fetched(self, url: str):
        """Mark URL as fetched.
        
        Args:
            url: URL to mark
        """
        if url in self._fetched:
            return
        
        # Add to in-memory set
        self._fetched.add(url)
        
        # Append to file
        if self._file_handle:
            try:
                self._file_handle.write(url + '\n')
                self._file_handle.flush()
            except Exception as e:
                logger.error(f"Failed to write URL to fetched_urls: {e}")
    
    def get_all_fetched(self) -> Set[str]:
        """Get set of all fetched URLs.
        
        Returns:
            Set of fetched URLs
        """
        return self._fetched.copy()
    
    def size(self) -> int:
        """Get number of fetched URLs."""
        return len(self._fetched)
    
    def close(self):
        """Close file handle."""
        if self._file_handle:
            try:
                self._file_handle.close()
                logger.info(f"Dedup store closed - total fetched: {len(self._fetched)}")
            except Exception as e:
                logger.error(f"Error closing dedup store: {e}")
