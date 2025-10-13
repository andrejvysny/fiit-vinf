"""Full-featured crawler with frontier, dedup, policy, and link extraction.

This crawler:
- Maintains a priority queue (frontier) of URLs to fetch
- Deduplicates URLs using a file-based seen set
- Enforces policy rules (allow/deny patterns) and robots.txt
- Fetches pages and extracts links
- Emits discoveries to spool for scraper consumption
- Respects depth limits and per-repo caps
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import logging
from pathlib import Path
from typing import List, Optional, Dict, Set
from collections import defaultdict

from .config import CrawlerConfig
from .io.discoveries import DiscoveriesWriter
from .frontier.frontier_file import FileFrontier
from .frontier.dedup import DedupStore
from .policy import UrlPolicy
from .robots_cache import RobotsCache
from .parse.extractor import LinkExtractor
from .net.fetcher import CrawlerFetcher
from .url_tools import canonicalize, extract_repo_info

logger = logging.getLogger(__name__)


class CrawlerService:
    """Full-featured crawler with BFS, dedup, policy, and link extraction."""

    def __init__(self, config: CrawlerConfig, **kwargs):
        """Initialize crawler service.
        
        Args:
            config: Crawler configuration
            **kwargs: Additional arguments (ignored for compatibility)
        """
        self.config = config
        self.workspace = config.get_workspace_path()
        self.spool_dir = Path(config.spool.discoveries_dir)
        
        # Components
        self.frontier: Optional[FileFrontier] = None
        self.dedup: Optional[DedupStore] = None
        self.policy: Optional[UrlPolicy] = None
        self.robots: Optional[RobotsCache] = None
        self.fetcher: Optional[CrawlerFetcher] = None
        self.extractor: Optional[LinkExtractor] = None
        self.writer: Optional[DiscoveriesWriter] = None
        
        # State
        self._stop = False
        self._running = False
        
        # Per-repo counters for caps enforcement
        self._repo_pages: Dict[str, int] = defaultdict(int)
        self._repo_issues: Dict[str, int] = defaultdict(int)
        self._repo_prs: Dict[str, int] = defaultdict(int)
        
        # Metrics
        self.stats = {
            "urls_fetched": 0,
            "links_extracted": 0,
            "discoveries_written": 0,
            "policy_denied": 0,
            "already_seen": 0,
            "fetch_errors": 0,
            "cap_exceeded": 0
        }
    
    async def start(self, seeds: List[str]) -> None:
        """Initialize components and seed frontier.
        
        Args:
            seeds: Initial seed URLs to crawl
        """
        logger.info("Initializing crawler components...")
        
        # Ensure directories exist
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.frontier = FileFrontier(self.config.frontier.db_path)
        self.dedup = DedupStore(str(self.workspace / "state" / "seen"))
        self.robots = RobotsCache(
            user_agent=self.config.robots.user_agent,
            cache_ttl_sec=self.config.robots.cache_ttl_sec
        )
        self.policy = UrlPolicy(self.config, self.robots)
        self.fetcher = CrawlerFetcher(self.config)
        self.extractor = LinkExtractor()
        self.writer = DiscoveriesWriter(str(self.spool_dir))
        
        # Seed frontier with initial URLs
        logger.info(f"Seeding frontier with {len(seeds)} URLs...")
        for seed in seeds:
            canonical = canonicalize(seed)
            if not self.dedup.is_seen(canonical):
                if not self.frontier.contains(canonical):
                    self.frontier.add(
                        canonical,
                        depth=0,
                        score=0.0,
                        page_type="seed"
                    )
                self.dedup.mark_seen(canonical)
                logger.info(f"Added seed: {canonical}")
        
        logger.info(f"Crawler initialized. Frontier size: {self.frontier.size()}")
    
    async def run(self) -> None:
        """Main crawler loop.
        
        Pops URLs from frontier, fetches pages, extracts links,
        checks policy, adds new URLs to frontier, and emits discoveries.
        """
        if self._running:
            logger.warning("Crawler already running")
            return
        
        self._running = True
        logger.info("Starting crawler main loop...")
        
        try:
            while not self._stop:
                # Check if frontier is empty
                if self.frontier.is_empty():
                    logger.info("Frontier empty - crawler waiting...")
                    await asyncio.sleep(5)
                    continue
                
                # Pop next URL
                url = self.frontier.pop()
                if not url:
                    await asyncio.sleep(0.1)
                    continue
                
                # Fetch and process
                await self._process_url(url)
                
                # Small delay to respect rate limit
                await asyncio.sleep(1.0 / self.config.limits.req_per_sec)
        
        except asyncio.CancelledError:
            logger.info("Crawler cancelled")
        finally:
            self._running = False
            logger.info("Crawler main loop stopped")
    
    async def _process_url(self, url: str):
        """Fetch URL, extract links, and enqueue new URLs.
        
        Args:
            url: URL to process
        """
        logger.debug(f"Processing: {url}")
        
        # Fetch page
        success, html, metadata = await self.fetcher.fetch(url)
        
        if not success:
            logger.debug(f"Failed to fetch {url}: {metadata}")
            self.stats["fetch_errors"] += 1
            return
        
        self.stats["urls_fetched"] += 1
        
        # Classify page type
        page_type = self.policy.classify(url)
        
        # Extract links
        extracted_links = self.extractor.extract(html, url)
        self.stats["links_extracted"] += len(extracted_links)
        
        logger.debug(f"Extracted {len(extracted_links)} links from {url}")
        
        # Calculate depth for child URLs
        # Get current depth from URL (we'll track this in frontier metadata later)
        # For now, estimate depth from path segments
        from .url_tools import get_url_depth
        current_depth = get_url_depth(url)
        child_depth = current_depth + 1
        
        # Check depth limit
        if child_depth > self.config.caps.max_depth:
            logger.debug(f"Depth limit reached for children of {url}")
        else:
            # Process each extracted link
            for link in extracted_links:
                await self._enqueue_url(link, url, child_depth)
        
        # Emit discovery for this URL
        self._emit_discovery(url, page_type, current_depth, metadata)
    
    async def _enqueue_url(self, url: str, referrer: str, depth: int):
        """Add URL to frontier if allowed and not seen.
        
        Args:
            url: URL to enqueue
            referrer: URL that linked to this one
            depth: Crawl depth
        """
        # Canonicalize
        canonical = canonicalize(url)
        
        # Check if already seen
        if self.dedup.is_seen(canonical):
            self.stats["already_seen"] += 1
            return
        
        # Check policy
        policy_result = await self.policy.gate(canonical)
        if not policy_result["ok"]:
            self.stats["policy_denied"] += 1
            logger.debug(f"Policy denied {canonical}: {policy_result['reason']}")
            return
        
        # Check caps (per-repo limits)
        if not self._check_caps(canonical, policy_result.get("page_type")):
            self.stats["cap_exceeded"] += 1
            logger.debug(f"Cap exceeded for {canonical}")
            return
        
        # Add to frontier and mark seen
        self.frontier.add(
            canonical,
            depth=depth,
            score=float(depth),
            page_type=policy_result.get("page_type"),
            referrer=referrer
        )
        self.dedup.mark_seen(canonical)
        
        logger.debug(f"Enqueued: {canonical} (depth={depth})")
    
    def _check_caps(self, url: str, page_type: Optional[str]) -> bool:
        """Check if URL exceeds per-repo caps.
        
        Args:
            url: URL to check
            page_type: Page type classification
            
        Returns:
            True if under caps, False if exceeded
        """
        # Extract repo info
        repo_info = extract_repo_info(url)
        if not repo_info:
            # Not a repo URL, allow
            return True
        
        owner, repo = repo_info
        repo_key = f"{owner}/{repo}"
        
        # Check page type limits
        if page_type == "issues":
            if self._repo_issues[repo_key] >= self.config.caps.per_repo_max_issues:
                return False
            self._repo_issues[repo_key] += 1
        
        elif page_type == "pull":
            if self._repo_prs[repo_key] >= self.config.caps.per_repo_max_prs:
                return False
            self._repo_prs[repo_key] += 1
        
        else:
            # General page
            if self._repo_pages[repo_key] >= self.config.caps.per_repo_max_pages:
                return False
            self._repo_pages[repo_key] += 1
        
        return True
    
    def _emit_discovery(self, url: str, page_type: Optional[str], depth: int, metadata: Dict):
        """Write discovery to spool.
        
        Args:
            url: URL discovered
            page_type: Page type classification
            depth: Crawl depth
            metadata: Fetch metadata
        """
        item = {
            "url": url,
            "page_type": page_type or "unknown",
            "depth": depth,
            "referrer": None,  # Could track this if needed
            "metadata": {
                "status_code": metadata.get("status_code"),
                "content_length": metadata.get("content_length"),
                "latency_ms": metadata.get("latency_ms")
            }
        }
        
        try:
            self.writer.write(item)
            self.stats["discoveries_written"] += 1
            logger.debug(f"Wrote discovery: {url}")
        except Exception as e:
            logger.error(f"Failed to write discovery for {url}: {e}")
    
    async def stop(self) -> None:
        """Stop crawler and cleanup resources."""
        logger.info("Stopping crawler...")
        self._stop = True
        
        # Wait a moment for loop to exit
        await asyncio.sleep(0.5)
        
        # Cleanup
        if self.frontier:
            self.frontier.close()
        
        if self.writer:
            self.writer.close()
        
        # Log final stats
        logger.info(f"Crawler stats: {self.stats}")
        logger.info(f"Fetcher stats: {self.fetcher.get_stats() if self.fetcher else {}}")
        logger.info("Crawler stopped")


def main():
    """CLI entry point for standalone crawler runs."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config.yaml')
    parser.add_argument('--seeds')
    parser.add_argument('--seed', action='append')
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    cfg = CrawlerConfig.from_yaml(args.config)

    # Collect seeds
    seeds = []
    if args.seeds:
        p = Path(args.seeds)
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.strip()
                if line and not line.startswith('#'):
                    seeds.append(line)
    if args.seed:
        seeds.extend(args.seed)
    
    # Add default seeds if none provided
    if not seeds:
        seeds = [
            'https://github.com/topics',
            'https://github.com/trending'
        ]

    service = CrawlerService(cfg)

    async def run_all():
        await service.start(seeds)
        try:
            await service.run()
        finally:
            await service.stop()

    asyncio.run(run_all())


if __name__ == '__main__':
    main()

