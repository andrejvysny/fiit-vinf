"""File-based frontier (priority queue) for URL crawling.

Replaces LMDB-based frontier with JSONL file storage.
Supports breadth-first traversal, resume after crash, and periodic compaction.
"""

import json
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
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


class FileFrontier:
    """File-based frontier for BFS URL crawling."""
    
    def __init__(self, frontier_path: str):
        """Initialize file-based frontier.
        
        Args:
            frontier_path: Path to frontier.jsonl file
        """
        self.frontier_path = Path(frontier_path)
        self.frontier_path.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory queue for performance
        self._queue: List[FrontierItem] = []
        self._queue_set: set = set()  # URLs in queue for fast lookup
        
        # Load existing frontier if present
        self._load()
        
        # Statistics
        self.urls_added = 0
        self.urls_popped = 0
        
        logger.info(f"Frontier initialized with {len(self._queue)} URLs")
    
    def _load(self):
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
        """Add URL to frontier if not already present.
        
        Args:
            url: URL to add
            depth: Crawl depth
            score: Priority score (lower = higher priority)
            page_type: Optional page type classification
            referrer: Optional referrer URL
        """
        # Check if already in queue
        if url in self._queue_set:
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
        """Check if URL is in frontier."""
        return url in self._queue_set
    
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
    
    def compact(self, fetched_urls: set):
        """Remove fetched URLs from frontier.
        
        This is called periodically to clean up the frontier file.
        
        Args:
            fetched_urls: Set of already-fetched URLs to remove
        """
        original_size = len(self._queue)
        
        # Filter out fetched URLs
        self._queue = [item for item in self._queue if item.url not in fetched_urls]
        self._queue_set = {item.url for item in self._queue}
        
        removed = original_size - len(self._queue)
        if removed > 0:
            logger.info(f"Compacted frontier: removed {removed} fetched URLs")
            self.persist()
    
    def close(self):
        """Close frontier and persist final state."""
        logger.info("Closing frontier...")
        self.persist()
        logger.info(f"Frontier stats - Added: {self.urls_added}, Popped: {self.urls_popped}, Remaining: {len(self._queue)}")
