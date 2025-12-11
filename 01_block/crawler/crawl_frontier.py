import json
import logging
import time
import hashlib
import shutil
from pathlib import Path
from typing import Optional, Iterable
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class FrontierItem:
    url: str
    depth: int
    score: float
    page_type: Optional[str] = None
    referrer: Optional[str] = None
    discovered_at: float = 0.0
    
    def __post_init__(self):
        if self.discovered_at == 0.0:
            self.discovered_at = time.time()


class CrawlFrontier:
    
    def __init__(self, frontier_path: str, fetched_urls_path: str):
        self.frontier_path = Path(frontier_path)
        self.frontier_path.parent.mkdir(parents=True, exist_ok=True)
        self.frontier_path.touch(exist_ok=True)

        self.fetched_urls_path = Path(fetched_urls_path)
        self.fetched_urls_path.parent.mkdir(parents=True, exist_ok=True)
        self.fetched_urls_path.touch(exist_ok=True)

        # Index directories for queue membership and fetched deduplication
        self._queue_index_dir = self.frontier_path.parent / f"{self.frontier_path.stem}_index"
        self._fetched_index_dir = self.fetched_urls_path.parent / f"{self.fetched_urls_path.stem}_index"
        self._queue_index_dir.mkdir(parents=True, exist_ok=True)
        self._fetched_index_dir.mkdir(parents=True, exist_ok=True)

        # Persistent metadata files
        self._cursor_path = self.frontier_path.with_suffix('.cursor')
        self._stats_path = self.frontier_path.with_suffix('.stats.json')

        # File handles
        self._queue_reader = None
        self._queue_writer = None
        self._fetched_file_handle = None

        # Counters
        self._queue_size = 0
        self._fetched_count = 0
        self._read_position = 0
        self.urls_added = 0
        self.urls_popped = 0

        # Load persisted stats (best effort)
        self._load_stats()

        # Initialize registries
        self._initialize_fetched_registry()
        self._open_fetched_for_append()
        self._initialize_queue_registry()

        # Restore cursor and open queue files
        self._read_position = self._load_cursor()
        self._open_queue_files()

        logger.info(
            "Crawl frontier initialized: %d pending URLs, %d fetched URLs",
            self._queue_size,
            self._fetched_count,
        )
    
    # =========================================================================
    # Deduplication (Fetched URLs Management)
    # =========================================================================
    
    def _initialize_fetched_registry(self) -> None:
        """Populate on-disk fetched URL index and count entries."""
        count = 0
        if self._fetched_index_dir.exists():
            shutil.rmtree(self._fetched_index_dir)
        self._fetched_index_dir.mkdir(parents=True, exist_ok=True)
        try:
            with self.fetched_urls_path.open('r', encoding='utf-8') as handle:
                for line in handle:
                    url = line.strip()
                    if not url:
                        continue
                    if self._ensure_index_marker(self._fetched_index_dir, url):
                        count += 1
        except FileNotFoundError:
            # File was just created; nothing to index yet
            self._fetched_count = 0
        except Exception as exc:
            logger.error("Failed to initialize fetched registry: %s", exc)
            self._fetched_count = 0
        else:
            self._fetched_count = count
    
    def _open_fetched_for_append(self):
        """Open fetched_urls file for append-only writes."""
        try:
            self._fetched_file_handle = self.fetched_urls_path.open('a', encoding='utf-8', buffering=1)
        except Exception as e:
            logger.error(f"Failed to open fetched_urls file for append: {e}")
            self._fetched_file_handle = None
    
    def is_fetched(self, url: str) -> bool:
        """Check if URL has been fetched.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL was already fetched
        """
        return self._index_exists(self._fetched_index_dir, url)
    
    def mark_fetched(self, url: str):
        """Mark URL as fetched.
        
        Args:
            url: URL to mark
        """
        if self._index_exists(self._fetched_index_dir, url):
            return
        
        if self._remove_index_marker(self._queue_index_dir, url):
            self._queue_size = max(0, self._queue_size - 1)
        marker_created = self._ensure_index_marker(self._fetched_index_dir, url)
        if marker_created:
            self._fetched_count += 1
        else:
            return
        
        # Append to file
        if self._fetched_file_handle is None:
            self._open_fetched_for_append()
        if self._fetched_file_handle:
            try:
                self._fetched_file_handle.write(url + '\n')
                self._fetched_file_handle.flush()
            except Exception as e:
                logger.error(f"Failed to write URL to fetched_urls: {e}")
    
    def get_all_fetched(self) -> Iterable[str]:
        """Stream fetched URLs from disk.

        Returns:
            Iterable over fetched URLs (consumers should iterate to avoid loading into memory).
        """
        try:
            with self.fetched_urls_path.open('r', encoding='utf-8') as handle:
                for line in handle:
                    url = line.strip()
                    if url:
                        yield url
        except FileNotFoundError:
            return
    
    def fetched_count(self) -> int:
        """Get number of fetched URLs."""
        return self._fetched_count
    
    # =========================================================================
    # Frontier Queue Management
    # =========================================================================
    
    def _initialize_queue_registry(self) -> None:
        """Rebuild the on-disk queue index from the frontier log."""
        if self._queue_index_dir.exists():
            shutil.rmtree(self._queue_index_dir)
        self._queue_index_dir.mkdir(parents=True, exist_ok=True)

        queue_size = 0
        try:
            with self.frontier_path.open('r', encoding='utf-8') as handle:
                for line in handle:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("Skipping malformed frontier line during index build")
                        continue
                    url = data.get('url')
                    if not url:
                        continue
                    if self.is_fetched(url):
                        continue
                    if self._ensure_index_marker(self._queue_index_dir, url):
                        queue_size += 1
        except FileNotFoundError:
            queue_size = 0
        except Exception as exc:
            logger.error("Failed to initialize queue registry: %s", exc)
        self._queue_size = queue_size

    def _open_queue_files(self) -> None:
        """Ensure queue reader and writer handles are open."""
        if self._queue_reader is None:
            self._queue_reader = self.frontier_path.open('r', encoding='utf-8')
            max_position = self.frontier_path.stat().st_size
            self._read_position = min(self._read_position, max_position)
            self._queue_reader.seek(self._read_position)
        if self._queue_writer is None:
            self._queue_writer = self.frontier_path.open('a', encoding='utf-8', buffering=1)
    
    def add(self, url: str, depth: int, score: float = 0.0, 
            page_type: Optional[str] = None, referrer: Optional[str] = None):
        """Add URL to frontier if not already present or fetched.
        
        Args:
            url: URL to add
            depth: Crawl depth
            score: Priority score (lower = higher priority)
            page_type: Optional page type classification
            referrer: Optional referrer URL
        """
        # Check if already in queue or already fetched
        if self._index_exists(self._queue_index_dir, url) or self.is_fetched(url):
            return
        
        # Create frontier item
        item = FrontierItem(
            url=url,
            depth=depth,
            score=score,
            page_type=page_type,
            referrer=referrer
        )
        
        line = json.dumps(asdict(item))
        if self._queue_writer is None:
            self._open_queue_files()
        try:
            self._queue_writer.write(line + '\n')
        except Exception as exc:
            logger.error("Failed to append to frontier: %s", exc)
            return

        if self._ensure_index_marker(self._queue_index_dir, url):
            self._queue_size += 1
            self.urls_added += 1
    
    def pop(self) -> Optional[str]:
        """Pop next URL from frontier (breadth-first order).
        
        Note: This does NOT automatically mark the URL as fetched.
        Use mark_fetched() after successful fetch.
        
        Returns:
            Next URL to crawl, or None if frontier is empty
        """
        if self._queue_size == 0:
            return None
        
        self._open_queue_files()
        while True:
            self._queue_reader.seek(self._read_position)
            line = self._queue_reader.readline()
            if not line:
                return None
            self._read_position = self._queue_reader.tell()
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipping malformed frontier line during pop")
                continue
            url = data.get('url')
            if not url:
                continue
            if self.is_fetched(url):
                continue
            if not self._index_exists(self._queue_index_dir, url):
                continue
            if self._remove_index_marker(self._queue_index_dir, url):
                self._queue_size = max(0, self._queue_size - 1)
            self.urls_popped += 1
            self._persist_cursor()
            return url
    
    def is_empty(self) -> bool:
        """Check if frontier is empty."""
        return self._queue_size == 0
    
    def size(self) -> int:
        """Get current frontier size."""
        return self._queue_size
    
    def contains(self, url: str) -> bool:
        """Check if URL is in frontier queue.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is currently in the frontier queue
        """
        return self._index_exists(self._queue_index_dir, url)
    
    # =========================================================================
    # Persistence and Cleanup
    # =========================================================================
    
    def persist(self):
        """Persist cursor and statistics for crash-safe resume."""
        try:
            self._flush_queue_writer()
            self._persist_cursor()
            self._persist_stats()
            logger.info("Persisted frontier state (queue_size=%d)", self._queue_size)
        except Exception as exc:
            logger.error("Failed to persist frontier: %s", exc)
    
    def compact(self):
        """Remove fetched URLs from the frontier log and rebuild index."""
        tmp_path = self.frontier_path.with_suffix('.tmp')
        kept = 0
        self._flush_queue_writer()
        try:
            with self.frontier_path.open('r', encoding='utf-8') as src, tmp_path.open('w', encoding='utf-8') as dst:
                for line in src:
                    raw = line.strip()
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    url = data.get('url')
                    if not url:
                        continue
                    if self.is_fetched(url):
                        continue
                    dst.write(line if line.endswith('\n') else line + '\n')
                    kept += 1
            tmp_path.replace(self.frontier_path)
            self._read_position = 0
            self._persist_cursor()
            self._reset_queue_handles()
            self._initialize_queue_registry()
            logger.info("Compacted frontier: kept %d URLs", kept)
        except Exception as exc:
            logger.error("Failed to compact frontier: %s", exc)
            if tmp_path.exists():
                tmp_path.unlink()
    
    def close(self):
        """Close frontier and persist final state."""
        logger.info("Closing crawl frontier...")
        
        # Persist frontier
        self.persist()
        
        # Close fetched URLs file handle
        if self._fetched_file_handle:
            try:
                self._fetched_file_handle.close()
                logger.info("Crawl frontier closed - total fetched: %d", self._fetched_count)
            except Exception as e:
                logger.error(f"Error closing fetched_urls file: {e}")
            finally:
                self._fetched_file_handle = None
        
        if self._queue_reader:
            self._queue_reader.close()
            self._queue_reader = None
        if self._queue_writer:
            self._queue_writer.close()
            self._queue_writer = None

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _index_marker_path(self, base_dir: Path, url: str) -> Path:
        digest = hashlib.sha256(url.encode('utf-8')).hexdigest()
        return base_dir / digest[:2] / digest[2:4] / digest

    def _ensure_index_marker(self, base_dir: Path, url: str) -> bool:
        marker = self._index_marker_path(base_dir, url)
        if marker.exists():
            return False
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.touch()
        return True

    def _remove_index_marker(self, base_dir: Path, url: str) -> bool:
        marker = self._index_marker_path(base_dir, url)
        if marker.exists():
            marker.unlink()
            self._prune_empty_dirs(base_dir, marker.parent)
            return True
        return False

    def _index_exists(self, base_dir: Path, url: str) -> bool:
        return self._index_marker_path(base_dir, url).exists()

    def _prune_empty_dirs(self, base_dir: Path, path: Path) -> None:
        try:
            while path != path.parent and path != base_dir and not any(path.iterdir()):
                path.rmdir()
                path = path.parent
        except OSError:
            return

    def _load_cursor(self) -> int:
        if not self._cursor_path.exists():
            try:
                self._cursor_path.write_text('0', encoding='utf-8')
            except Exception:
                return 0
            return 0
        try:
            value = self._cursor_path.read_text(encoding='utf-8').strip() or '0'
            position = int(value)
            return max(0, position)
        except Exception as exc:
            logger.warning("Falling back to cursor=0 due to error: %s", exc)
            return 0

    def _persist_cursor(self) -> None:
        try:
            self._cursor_path.write_text(str(self._read_position), encoding='utf-8')
        except Exception as exc:
            logger.error("Failed to persist frontier cursor: %s", exc)

    def _load_stats(self) -> None:
        if not self._stats_path.exists():
            return
        try:
            payload = json.loads(self._stats_path.read_text(encoding='utf-8'))
            self.urls_added = int(payload.get('urls_added', 0))
            self.urls_popped = int(payload.get('urls_popped', 0))
        except Exception as exc:
            logger.warning("Failed to load frontier stats: %s", exc)
            self.urls_added = 0
            self.urls_popped = 0

    def _persist_stats(self) -> None:
        payload = {
            'urls_added': self.urls_added,
            'urls_popped': self.urls_popped,
            'queue_size': self._queue_size,
            'fetched_count': self._fetched_count,
        }
        try:
            self._stats_path.write_text(json.dumps(payload), encoding='utf-8')
        except Exception as exc:
            logger.error("Failed to persist frontier stats: %s", exc)

    def _flush_queue_writer(self) -> None:
        if self._queue_writer:
            try:
                self._queue_writer.flush()
            except Exception as exc:
                logger.error("Failed to flush frontier writer: %s", exc)

    def _reset_queue_handles(self) -> None:
        if self._queue_reader:
            self._queue_reader.close()
            self._queue_reader = None
        if self._queue_writer:
            self._queue_writer.close()
            self._queue_writer = None
        self._open_queue_files()
