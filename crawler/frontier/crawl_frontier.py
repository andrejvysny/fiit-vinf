"""Unified frontier and deduplication for URL crawling.

Merges FileFrontier and FileDedupStore into a single module that:
- Manages priority queue for URLs (BFS by depth)
- Tracks fetched URLs for deduplication
- Persists both to disk (frontier.jsonl and fetched_urls.txt)
- Provides atomic operations for URL lifecycle
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class FrontierItem:
    """Represents a URL in the frontier queue."""
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
    """Unified frontier and deduplication store for URL crawling.
    
    Combines priority queue management with deduplication tracking
    for streamlined URL lifecycle management.
    """
    
    def __init__(self, frontier_path: str, fetched_urls_path: str):
        """Initialize crawl frontier with deduplication.
        
        Args:
            frontier_path: Path to frontier.jsonl file
            fetched_urls_path: Path to fetched_urls.txt file
        """
        # Frontier setup
        self.frontier_path = Path(frontier_path)
        self.frontier_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Dedup setup
        self.fetched_urls_path = Path(fetched_urls_path)
        self.fetched_urls_path.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory queue for performance (frontier)
        self._queue: List[FrontierItem] = []
        self._queue_set: set = set()  # URLs in queue for fast lookup
        
        # In-memory set for deduplication (fetched URLs)
        self._fetched: Set[str] = set()
        
        # File handle for append-only fetched URLs
        self._fetched_file_handle = None
        
        # Load existing data
        self._load_fetched_urls()
        self._load_frontier()
        self._open_fetched_for_append()
        
        # Statistics
        self.urls_added = 0
        self.urls_popped = 0
        
        logger.info(
            f"Crawl frontier initialized: "
            f"{len(self._queue)} URLs in queue, "
            f"{len(self._fetched)} URLs fetched"
        )
    
    # =========================================================================
    # Deduplication (Fetched URLs Management)
    # =========================================================================
    
    def _load_fetched_urls(self):
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
    
    def _open_fetched_for_append(self):
        """Open fetched_urls file for append-only writes."""
        try:
            self._fetched_file_handle = open(self.fetched_urls_path, 'a', buffering=1)  # Line buffered
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
        if self._fetched_file_handle:
            try:
                self._fetched_file_handle.write(url + '\n')
                self._fetched_file_handle.flush()
            except Exception as e:
                logger.error(f"Failed to write URL to fetched_urls: {e}")
    
    def get_all_fetched(self) -> Set[str]:
        """Get set of all fetched URLs.
        
        Returns:
            Set of fetched URLs
        """
        return self._fetched.copy()
    
    def fetched_count(self) -> int:
        """Get number of fetched URLs."""
        return len(self._fetched)
    
    # =========================================================================
    # Frontier Queue Management
    # =========================================================================
    
    def _load_frontier(self):
        """Load frontier from disk into memory."""
        if not self.frontier_path.exists():
            logger.info("No existing frontier file found - starting fresh")
            return
        
        try:
            with open(self.frontier_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        data = json.loads(line)
                        item = FrontierItem(**data)
                        
                        # Skip if already fetched (resume scenario)
                        if item.url not in self._fetched:
                            self._queue.append(item)
                            self._queue_set.add(item.url)
                    except Exception as e:
                        logger.error(f"Failed to parse frontier line: {e}")
            
            # Sort by depth (breadth-first)
            self._queue.sort(key=lambda x: (x.depth, x.score, x.discovered_at))
            
            logger.info(f"Loaded {len(self._queue)} URLs from frontier")
            
        except Exception as e:
            logger.error(f"Failed to load frontier: {e}")
            self._queue = []
            self._queue_set = set()
    
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
        if url in self._queue_set or url in self._fetched:
            return
        
        # Create frontier item
        item = FrontierItem(
            url=url,
            depth=depth,
            score=score,
            page_type=page_type,
            referrer=referrer
        )
        
        # Add to in-memory queue
        self._queue.append(item)
        self._queue_set.add(url)
        self.urls_added += 1
        
        # Maintain sorted order (insert in correct position for BFS)
        # For efficiency, we'll sort periodically instead of on each insert
        if len(self._queue) % 100 == 0:
            self._sort_queue()
    
    def _sort_queue(self):
        """Sort queue for breadth-first order."""
        self._queue.sort(key=lambda x: (x.depth, x.score, x.discovered_at))
    
    def pop(self) -> Optional[str]:
        """Pop next URL from frontier (breadth-first order).
        
        Note: This does NOT automatically mark the URL as fetched.
        Use mark_fetched() after successful fetch.
        
        Returns:
            Next URL to crawl, or None if frontier is empty
        """
        if not self._queue:
            return None
        
        # Ensure sorted (in case adds happened since last sort)
        if self.urls_added % 100 == 0:
            self._sort_queue()
        
        # Pop first item (lowest depth = BFS)
        item = self._queue.pop(0)
        self._queue_set.discard(item.url)
        self.urls_popped += 1
        
        return item.url
    
    def is_empty(self) -> bool:
        """Check if frontier is empty."""
        return len(self._queue) == 0
    
    def size(self) -> int:
        """Get current frontier size."""
        return len(self._queue)
    
    def contains(self, url: str) -> bool:
        """Check if URL is in frontier queue.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL is currently in the frontier queue
        """
        return url in self._queue_set
    
    # =========================================================================
    # Persistence and Cleanup
    # =========================================================================
    
    def persist(self):
        """Persist frontier to disk (full rewrite).
        
        This is called periodically or on shutdown to save state.
        """
        try:
            # Write to temp file first (atomic)
            tmp_path = self.frontier_path.with_suffix('.tmp')
            
            with open(tmp_path, 'w') as f:
                for item in self._queue:
                    f.write(json.dumps(asdict(item)) + '\n')
            
            # Atomic rename
            tmp_path.replace(self.frontier_path)
            
            logger.info(f"Persisted frontier with {len(self._queue)} URLs")
            
        except Exception as e:
            logger.error(f"Failed to persist frontier: {e}")
    
    def compact(self):
        """Remove fetched URLs from frontier.
        
        This is called periodically to clean up the frontier file.
        Note: The fetched URLs are already in self._fetched, so we
        just filter against it.
        """
        original_size = len(self._queue)
        
        # Filter out fetched URLs
        self._queue = [item for item in self._queue if item.url not in self._fetched]
        self._queue_set = {item.url for item in self._queue}
        
        removed = original_size - len(self._queue)
        if removed > 0:
            logger.info(f"Compacted frontier: removed {removed} fetched URLs")
            self.persist()
    
    def close(self):
        """Close frontier and persist final state."""
        logger.info("Closing crawl frontier...")
        
        # Persist frontier
        self.persist()
        
        # Close fetched URLs file handle
        if self._fetched_file_handle:
            try:
                self._fetched_file_handle.close()
                logger.info(f"Crawl frontier closed - total fetched: {len(self._fetched)}")
            except Exception as e:
                logger.error(f"Error closing fetched_urls file: {e}")
