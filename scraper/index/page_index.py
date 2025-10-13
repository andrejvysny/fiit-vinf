"""File-based page index for conditional requests and deduplication."""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Optional, Dict, Any, Iterator, Tuple


class PageIndex:
    """Append-only JSONL index keyed by canonical URL.

    Stores metadata used for conditional requests and deduplication without
    relying on LMDB. The index is loaded into memory on startup and persisted
    via an append-only log with periodic compaction.
    """

    _COMPACT_THRESHOLD = 1000  # Number of writes before compacting

    def __init__(self, db_path: str):
        """Initialize file-based page index.

        Args:
            db_path: Path to JSONL file storing index records
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.RLock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._tombstones: set[str] = set()
        self._log_handle = None
        self._writes_since_compact = 0

        # Statistics
        self.gets = 0
        self.puts = 0
        self.hits = 0

        self._load()
        self._open_log()

    def _load(self) -> None:
        """Load index data from disk into memory."""
        if not self.db_path.exists():
            # Create empty file for subsequent appends
            self.db_path.touch()
            return

        try:
            with self.db_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    url = record.get("url")
                    if not url:
                        continue

                    if record.get("__deleted__"):
                        self._data.pop(url, None)
                        self._tombstones.add(url)
                        continue

                    self._data[url] = record
                    self._tombstones.discard(url)
        except Exception as exc:
            print(f"ERROR: Failed to load page index: {exc}")
            self._data.clear()
            self._tombstones.clear()

    def _open_log(self) -> None:
        """Open the log file for append operations."""
        try:
            self._log_handle = self.db_path.open("a", encoding="utf-8")
        except Exception as exc:
            print(f"ERROR: Failed to open page index file for append: {exc}")
            self._log_handle = None

    def _append_record(self, record: Dict[str, Any]) -> None:
        """Append a single JSON record to the log and flush it."""
        if not self._log_handle:
            return

        json_record = json.dumps(record, separators=(",", ":"))
        self._log_handle.write(json_record + "\n")
        self._log_handle.flush()

    def _maybe_compact(self) -> None:
        """Rewrite the index file to keep it bounded."""
        if self._writes_since_compact < self._COMPACT_THRESHOLD:
            return

        tmp_path = self.db_path.with_suffix(".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as tmp:
                for record in self._data.values():
                    tmp.write(json.dumps(record, separators=(",", ":")) + "\n")

            # Close current log before replacing
            if self._log_handle:
                self._log_handle.close()

            tmp_path.replace(self.db_path)
            self._open_log()
            self._writes_since_compact = 0
        except Exception as exc:
            print(f"ERROR: Failed to compact page index: {exc}")
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass

    def get(self, url: str) -> Optional[Dict[str, Any]]:
        """Get page metadata by URL."""
        with self._lock:
            self.gets += 1
            record = self._data.get(url)
            if record:
                self.hits += 1
                return dict(record)
            return None

    def put(self, record: Dict[str, Any]) -> bool:
        """Store or update page metadata."""
        if "url" not in record:
            raise ValueError("Record must have 'url' field")

        url = record["url"]

        with self._lock:
            self.puts += 1
            self._data[url] = dict(record)
            self._tombstones.discard(url)
            self._append_record(record)
            self._writes_since_compact += 1
            self._maybe_compact()
        return True

    def delete(self, url: str) -> bool:
        """Delete page metadata by URL."""
        with self._lock:
            if url not in self._data:
                return False

            self._data.pop(url, None)
            self._tombstones.add(url)
            tombstone = {"url": url, "__deleted__": True}
            self._append_record(tombstone)
            self._writes_since_compact += 1
            self._maybe_compact()
            return True

    def exists(self, url: str) -> bool:
        """Check if URL exists in index."""
        with self._lock:
            return url in self._data

    def size(self) -> int:
        """Get number of entries in index."""
        with self._lock:
            return len(self._data)

    def iterate(self, prefix: Optional[str] = None) -> Iterator[Tuple[str, Dict[str, Any]]]:
        """Iterate over entries, optionally filtered by URL prefix."""
        with self._lock:
            items = list(self._data.items())

        for url, record in items:
            if prefix and not url.startswith(prefix):
                continue
            yield url, dict(record)

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        with self._lock:
            file_size = self.db_path.stat().st_size if self.db_path.exists() else 0
            return {
                "entries": len(self._data),
                "file_size": file_size,
                "gets": self.gets,
                "puts": self.puts,
                "hits": self.hits,
                "hit_rate": f"{(self.hits / self.gets * 100):.1f}%" if self.gets else "N/A",
                "tombstones": len(self._tombstones)
            }

    def close(self) -> None:
        """Close the index and release resources."""
        with self._lock:
            if self._log_handle:
                self._log_handle.flush()
                self._log_handle.close()
                self._log_handle = None

    def sync(self) -> None:
        """Force sync to disk."""
        with self._lock:
            if self._log_handle:
                self._log_handle.flush()
                os.fsync(self._log_handle.fileno())

    def __enter__(self) -> "PageIndex":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()