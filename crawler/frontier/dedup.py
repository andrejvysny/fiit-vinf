"""File-based URL deduplication store.

Uses a file-sharded approach with touch files to track seen URLs.
Each URL is mapped to a hash and stored as an empty file in a
two-level directory structure for efficient lookups.
"""

import os
import hashlib
import tempfile
from pathlib import Path
from typing import Optional


class DedupStore:
    """File-based URL deduplication using touch files."""
    
    def __init__(self, base_dir: str):
        """Initialize dedup store.
        
        Args:
            base_dir: Base directory for seen files (e.g., workspace/state/seen)
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def _url_key(self, url: str) -> str:
        """Compute hash key for URL.
        
        Args:
            url: URL to hash
            
        Returns:
            Hex digest of sha256(url)
        """
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
    
    def _path_for_url(self, url: str) -> Path:
        """Get file path for URL's seen marker.
        
        Uses two-level sharding: aa/bb/<hash>.seen
        
        Args:
            url: URL to get path for
            
        Returns:
            Path to touch file
        """
        key = self._url_key(url)
        # Two-level shard: first 2 chars, next 2 chars
        a = key[:2]
        b = key[2:4]
        
        shard_dir = self.base_dir / a / b
        shard_dir.mkdir(parents=True, exist_ok=True)
        
        return shard_dir / f"{key}.seen"
    
    def is_seen(self, url: str) -> bool:
        """Check if URL has been seen.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL exists in seen set
        """
        path = self._path_for_url(url)
        return path.exists()
    
    def mark_seen(self, url: str) -> bool:
        """Mark URL as seen.
        
        Creates an empty touch file atomically.
        
        Args:
            url: URL to mark as seen
            
        Returns:
            True if newly marked (was not seen before), False if already seen
        """
        path = self._path_for_url(url)
        
        # Check if already exists
        if path.exists():
            return False
        
        # Create atomically using temp + replace
        dirpath = path.parent
        fd, tmp = tempfile.mkstemp(dir=dirpath, suffix=".tmp")
        try:
            os.close(fd)
            os.replace(tmp, path)
            return True
        except FileExistsError:
            # Race condition - another process created it
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            return False
        except Exception:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            raise
    
    def size_estimate(self) -> int:
        """Estimate number of seen URLs.
        
        Counts all .seen files recursively.
        
        Returns:
            Approximate count of seen URLs
        """
        count = 0
        try:
            for root, dirs, files in os.walk(self.base_dir):
                count += sum(1 for f in files if f.endswith(".seen"))
        except Exception:
            pass
        return count
    
    def clear(self):
        """Remove all seen markers.
        
        WARNING: This deletes all .seen files.
        """
        import shutil
        if self.base_dir.exists():
            try:
                shutil.rmtree(self.base_dir)
                self.base_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise RuntimeError(f"Failed to clear dedup store: {e}")
