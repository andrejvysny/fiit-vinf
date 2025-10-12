"""
LMDB-based page index for conditional requests and deduplication
"""

import json
import lmdb
import os
from pathlib import Path
from typing import Optional, Dict, Any


class PageIndex:
    """
    LMDB index storing page metadata for conditional requests.

    Key: canonical URL
    Value: JSON with {url, sha256, stored_path, etag, last_modified, timestamp}
    """

    def __init__(self, db_path: str, map_size: int = 2 * 1024**3):
        """
        Initialize LMDB page index.

        Args:
            db_path: Path to LMDB database directory
            map_size: Maximum database size (default: 2GB)
        """
        self.db_path = Path(db_path)
        self.map_size = map_size

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Open LMDB environment
        self.env = lmdb.open(
            str(self.db_path),
            map_size=self.map_size,
            subdir=True,
            max_dbs=1,
            lock=True,
            metasync=True,
            sync=True,
            map_async=False,
            readahead=True,
            writemap=False
        )

        # Open named database
        self.db = self.env.open_db(b"page_index", create=True)

        # Statistics
        self.gets = 0
        self.puts = 0
        self.hits = 0

    def get(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get page metadata by URL.

        Args:
            url: Canonical URL to lookup

        Returns:
            Dict with page metadata or None if not found
        """
        self.gets += 1

        try:
            with self.env.begin(db=self.db, write=False) as txn:
                value = txn.get(url.encode('utf-8'))
                if value:
                    self.hits += 1
                    return json.loads(value.decode('utf-8'))
                return None
        except Exception as e:
            print(f"ERROR: Failed to get from index: {e}")
            return None

    def put(self, record: Dict[str, Any]) -> bool:
        """
        Store or update page metadata.

        Args:
            record: Dict with at minimum 'url' key

        Returns:
            True if successful
        """
        self.puts += 1

        if 'url' not in record:
            raise ValueError("Record must have 'url' field")

        try:
            url = record['url']
            value = json.dumps(record, separators=(',', ':'))

            with self.env.begin(db=self.db, write=True) as txn:
                txn.put(
                    url.encode('utf-8'),
                    value.encode('utf-8'),
                    overwrite=True
                )
            return True
        except Exception as e:
            print(f"ERROR: Failed to put to index: {e}")
            return False

    def delete(self, url: str) -> bool:
        """
        Delete page metadata by URL.

        Args:
            url: URL to delete

        Returns:
            True if deleted, False if not found or error
        """
        try:
            with self.env.begin(db=self.db, write=True) as txn:
                return txn.delete(url.encode('utf-8'))
        except Exception as e:
            print(f"ERROR: Failed to delete from index: {e}")
            return False

    def exists(self, url: str) -> bool:
        """Check if URL exists in index"""
        return self.get(url) is not None

    def size(self) -> int:
        """Get number of entries in index"""
        with self.env.begin(db=self.db, write=False) as txn:
            return txn.stat()['entries']

    def iterate(self, prefix: str = None):
        """
        Iterate over entries, optionally filtered by URL prefix.

        Args:
            prefix: Optional URL prefix to filter by

        Yields:
            Tuples of (url, record)
        """
        with self.env.begin(db=self.db, write=False) as txn:
            cursor = txn.cursor()

            if prefix:
                # Seek to prefix
                prefix_bytes = prefix.encode('utf-8')
                cursor.set_range(prefix_bytes)

                for key, value in cursor:
                    if not key.startswith(prefix_bytes):
                        break
                    url = key.decode('utf-8')
                    record = json.loads(value.decode('utf-8'))
                    yield url, record
            else:
                # Iterate all entries
                for key, value in cursor:
                    url = key.decode('utf-8')
                    record = json.loads(value.decode('utf-8'))
                    yield url, record

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        with self.env.begin(db=self.db, write=False) as txn:
            db_stats = txn.stat()

        env_stats = self.env.stat()

        return {
            "entries": db_stats['entries'],
            "db_size": db_stats['psize'] * (db_stats['leaf_pages'] + db_stats['overflow_pages']),
            "gets": self.gets,
            "puts": self.puts,
            "hits": self.hits,
            "hit_rate": f"{(self.hits / self.gets * 100):.1f}%" if self.gets > 0 else "N/A",
            "page_size": env_stats['psize'],
            "map_size": self.map_size
        }

    def close(self):
        """Close LMDB environment"""
        self.env.close()

    def sync(self):
        """Force sync to disk"""
        self.env.sync()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()