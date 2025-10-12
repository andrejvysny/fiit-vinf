"""URL frontier (priority queue) using LMDB for persistence.

Maintains a priority queue of URLs to crawl, with scores determining
fetch order. Lower scores are fetched first (BFS-like if depth is used).
"""

import lmdb
import struct
import logging
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class Frontier:
    """LMDB-backed priority queue for URLs.
    
    Keys are score+counter (for stable ordering), values are URLs.
    Pop returns lowest-score URL first.
    """
    
    def __init__(self, db_path: str, map_size: int = 10 * 1024**3):
        """Initialize frontier.
        
        Args:
            db_path: Path to LMDB database directory
            map_size: Maximum database size in bytes (default 10GB)
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Open LMDB environment
        self.env = lmdb.open(
            str(self.db_path),
            map_size=map_size,
            max_dbs=1,
            writemap=True,
            sync=True
        )
        
        # Counter for stable ordering (monotonic)
        self._counter = 0
        self._load_counter()
        
        logger.info(f"Frontier initialized at {self.db_path}, counter={self._counter}")
    
    def _load_counter(self):
        """Load counter from metadata key."""
        with self.env.begin() as txn:
            val = txn.get(b"__meta:counter")
            if val:
                self._counter = struct.unpack(">Q", val)[0]
    
    def _save_counter(self, txn):
        """Save counter to metadata key."""
        txn.put(b"__meta:counter", struct.pack(">Q", self._counter))
    
    def _make_key(self, score: float) -> bytes:
        """Create a composite key from score and counter.
        
        Format: score (double, 8 bytes) + counter (uint64, 8 bytes)
        Total: 16 bytes
        
        Args:
            score: Priority score (lower = higher priority)
            
        Returns:
            16-byte key
        """
        self._counter += 1
        # Pack as big-endian for lexicographic order
        return struct.pack(">dQ", score, self._counter)
    
    def add(self, url: str, score: float = 0.0):
        """Add URL to frontier with given score.
        
        Args:
            url: URL to add
            score: Priority score (lower = fetched sooner, default 0.0)
        """
        key = self._make_key(score)
        value = url.encode("utf-8")
        
        with self.env.begin(write=True) as txn:
            txn.put(key, value)
            self._save_counter(txn)
    
    def pop(self) -> Optional[str]:
        """Pop and return lowest-score URL.
        
        Returns:
            URL string or None if frontier is empty
        """
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor()
            # Move to first non-metadata key
            if not cursor.first():
                return None
            
            # Skip metadata keys
            while cursor.key().startswith(b"__meta:"):
                if not cursor.next():
                    return None
            
            key = cursor.key()
            value = cursor.value()
            
            # Delete this entry
            cursor.delete()
            
            return value.decode("utf-8")
    
    def peek(self) -> Optional[Tuple[float, str]]:
        """Peek at lowest-score URL without removing it.
        
        Returns:
            Tuple of (score, url) or None if empty
        """
        with self.env.begin() as txn:
            cursor = txn.cursor()
            if not cursor.first():
                return None
            
            # Skip metadata
            while cursor.key().startswith(b"__meta:"):
                if not cursor.next():
                    return None
            
            key = cursor.key()
            value = cursor.value()
            
            # Unpack score from key
            score = struct.unpack(">d", key[:8])[0]
            url = value.decode("utf-8")
            
            return (score, url)
    
    def size(self) -> int:
        """Get number of URLs in frontier.
        
        Returns:
            Count of pending URLs
        """
        with self.env.begin() as txn:
            stat = txn.stat()
            # Subtract metadata keys (currently just counter)
            return max(0, stat["entries"] - 1)
    
    def is_empty(self) -> bool:
        """Check if frontier is empty.
        
        Returns:
            True if no URLs pending
        """
        return self.size() == 0
    
    def close(self):
        """Close the frontier database."""
        try:
            self.env.close()
            logger.info("Frontier closed")
        except Exception as e:
            logger.error(f"Error closing frontier: {e}")
    
    def clear(self):
        """Remove all entries from frontier.
        
        WARNING: Deletes all pending URLs.
        """
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor()
            # Delete all entries
            count = 0
            for key, _ in cursor:
                if not key.startswith(b"__meta:"):
                    cursor.delete()
                    count += 1
            logger.info(f"Cleared {count} URLs from frontier")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
