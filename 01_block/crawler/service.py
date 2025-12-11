import asyncio
import json
import logging
import time
import random
from pathlib import Path
from typing import List, Optional, Dict
from collections import defaultdict

from .config import CrawlerScraperConfig
from .crawl_frontier import CrawlFrontier
from .crawl_policy import CrawlPolicy
from .extractor import LinkExtractor
from .unified_fetcher import UnifiedFetcher
from .metadata_writer import UnifiedMetadataWriter
from .url_tools import canonicalize, extract_repo_info

logger = logging.getLogger(__name__)


class CrawlerScraperService:

    def __init__(self, config: CrawlerScraperConfig):        
        self.config = config
        self.workspace = config.get_workspace_path()
        
        self.frontier: Optional[CrawlFrontier] = None
        self.policy: Optional[CrawlPolicy] = None
        self.fetcher: Optional[UnifiedFetcher] = None
        self.extractor: Optional[LinkExtractor] = None
        self.metadata_writer: Optional[UnifiedMetadataWriter] = None
        
        self._stop = False
        self._running = False
        
        self._repo_pages: Dict[str, int] = defaultdict(int)
        self._repo_issues: Dict[str, int] = defaultdict(int)
        self._repo_prs: Dict[str, int] = defaultdict(int)
        
        self._url_depth: Dict[str, int] = {}
        self._url_referrer: Dict[str, str] = {}
        
        self.stats = {
            "urls_fetched": 0,
            "urls_stored": 0,
            "links_extracted": 0,
            "urls_enqueued": 0,
            "policy_denied": 0,
            "already_fetched": 0,
            "fetch_errors": 0,
            "cap_exceeded": 0
        }
        
        # Runtime tracking
        self.start_time = None
        self._stats_path = None
        
        # Periodic save interval
        self._last_frontier_save = time.time()
        self._last_stats_save = time.time()
        self._frontier_save_interval = 60  # Save frontier every 60 seconds
        self._stats_save_interval = 60  # Save service stats every 60 seconds
        
        # Dynamic sleep tracking for batch pauses
        self._request_counter = 0
    
    def _calculate_html_files_count(self) -> int:
        """Count total number of stored HTML files.
        
        Returns:
            Number of HTML files in storage
        """
        store_dir = self.workspace / "store" / "html"
        if not store_dir.exists():
            return 0
        
        count = 0
        try:
            for html_file in store_dir.rglob("*.html"):
                if html_file.is_file():
                    count += 1
        except Exception as exc:
            logger.warning(f"Failed to count HTML files: {exc}")
        
        return count
    
    def _calculate_html_storage_bytes(self) -> int:
        """Calculate total storage size of HTML files in bytes.
        
        Returns:
            Total bytes used by HTML storage
        """
        store_dir = self.workspace / "store" / "html"
        if not store_dir.exists():
            return 0
        
        total_bytes = 0
        try:
            for html_file in store_dir.rglob("*.html"):
                if html_file.is_file():
                    total_bytes += html_file.stat().st_size
        except Exception as exc:
            logger.warning(f"Failed to calculate storage size: {exc}")
        
        return total_bytes
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes into human-readable string.
        
        Args:
            bytes_value: Number of bytes
            
        Returns:
            Formatted string (e.g., "1.5 GB", "234.2 MB")
        """
        if bytes_value < 1024:
            return f"{bytes_value} B"
        elif bytes_value < 1024 * 1024:
            return f"{bytes_value / 1024:.2f} KB"
        elif bytes_value < 1024 * 1024 * 1024:
            return f"{bytes_value / (1024 * 1024):.2f} MB"
        else:
            return f"{bytes_value / (1024 * 1024 * 1024):.2f} GB"
    
    def _get_runtime_seconds(self) -> float:
        """Get current runtime in seconds.
        
        Returns:
            Runtime in seconds since start, or 0 if not started
        """
        if self.start_time is None:
            return 0.0
        return time.time() - self.start_time
    
    def _get_acceptance_rate(self) -> float:
        """Calculate URL acceptance rate (enqueued / total evaluated).
        
        Returns:
            Acceptance rate as percentage (0-100)
        """
        total_evaluated = self.stats["urls_enqueued"] + self.stats["policy_denied"]
        if total_evaluated == 0:
            return 0.0
        return (self.stats["urls_enqueued"] / total_evaluated) * 100.0
    
    def _load_service_stats(self) -> None:
        """Load persisted service statistics from disk.
        
        Loads stats from service_stats.json and updates self.stats.
        Also restores start_time for accurate runtime tracking across restarts.
        """
        if self._stats_path is None or not self._stats_path.exists():
            return
        
        try:
            payload = json.loads(self._stats_path.read_text(encoding='utf-8'))
            
            # Restore counters
            for key in self.stats.keys():
                if key in payload:
                    self.stats[key] = int(payload[key])
            
            # Restore start_time for runtime tracking
            if "start_time" in payload:
                self.start_time = float(payload["start_time"])
            
            logger.info("Loaded service statistics from previous session")
        except Exception as exc:
            logger.warning(f"Failed to load service stats: {exc}")
    
    def _persist_service_stats(self) -> None:
        """Persist service statistics to disk.
        
        Saves current stats along with computed metrics to service_stats.json.
        """
        if self._stats_path is None:
            return
        
        # Calculate current metrics
        html_files = self._calculate_html_files_count()
        storage_bytes = self._calculate_html_storage_bytes()
        runtime_seconds = self._get_runtime_seconds()
        acceptance_rate = self._get_acceptance_rate()
        
        payload = {
            # Core counters
            **self.stats,
            
            # Runtime tracking
            "start_time": self.start_time if self.start_time else time.time(),
            "runtime_seconds": runtime_seconds,
            
            # Storage metrics
            "html_files_count": html_files,
            "html_storage_bytes": storage_bytes,
            "html_storage_formatted": self._format_bytes(storage_bytes),
            
            # Derived metrics
            "acceptance_rate_percent": acceptance_rate,
            
            # Timestamp
            "last_updated": time.time()
        }
        
        try:
            self._stats_path.write_text(json.dumps(payload, indent=2), encoding='utf-8')
            logger.debug("Persisted service statistics")
        except Exception as exc:
            logger.error(f"Failed to persist service stats: {exc}")
    
    async def start(self, seeds: List[str]) -> None:
        """Initialize components and seed frontier.
        
        Args:
            seeds: Initial seed URLs to crawl
        """
        logger.info("Initializing unified crawler components...")
        
        # Initialize runtime tracking
        if self.start_time is None:
            self.start_time = time.time()
        
        # Ensure directories exist
        state_dir = self.workspace / "state"
        metadata_dir = self.workspace / "metadata"
        store_dir = self.workspace / "store" / "html"
        
        state_dir.mkdir(parents=True, exist_ok=True)
        metadata_dir.mkdir(parents=True, exist_ok=True)
        store_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize stats persistence
        self._stats_path = state_dir / "service_stats.json"
        self._load_service_stats()
        
        # Initialize unified crawl frontier (includes deduplication)
        frontier_path = state_dir / "frontier.jsonl"
        dedup_path = state_dir / "fetched_urls.txt"
        self.frontier = CrawlFrontier(str(frontier_path), str(dedup_path))
        
        # Initialize unified crawl policy (includes robots.txt cache)
        robots_path = state_dir / "robots_cache.jsonl"
        self.policy = CrawlPolicy(
            self.config,
            cache_path=str(robots_path),
            user_agent=self.config.robots.user_agent,
            cache_ttl_sec=self.config.robots.cache_ttl_sec
        )
        
        # Initialize unified fetcher (fetch + store in one operation)
        self.fetcher = UnifiedFetcher(
            store_root=str(store_dir),
            user_agent=self.config.user_agent,
            user_agents=self.config.user_agents,
            user_agent_rotation_size=self.config.user_agent_rotation_size,
            accept_language=self.config.accept_language,
            accept_encoding=self.config.accept_encoding,
            connect_timeout_ms=self.config.limits.connect_timeout_ms,
            read_timeout_ms=self.config.limits.read_timeout_ms,
            max_retries=self.config.limits.max_retries,
            backoff_base_ms=self.config.limits.backoff_base_ms,
            backoff_cap_ms=self.config.limits.backoff_cap_ms,
            req_per_sec=self.config.limits.req_per_sec,
            compress=False,  # Can be configured
            permissions=0o644,
            proxy_config=self.config.proxies
        )
        
        # Initialize link extractor
        self.extractor = LinkExtractor()
        
        # Initialize unified metadata writer
        metadata_path = metadata_dir / "crawl_metadata.jsonl"
        self.metadata_writer = UnifiedMetadataWriter(str(metadata_path))
        
        # Seed frontier with initial URLs
        logger.info(f"Seeding frontier with {len(seeds)} URLs...")
        for seed in seeds:
            canonical = canonicalize(seed)
            
            # Check if already fetched (resume support)
            if self.frontier.is_fetched(canonical):
                logger.info(f"Seed already fetched (resume): {canonical}")
                continue
            
            # Add to frontier if not already there
            if not self.frontier.contains(canonical):
                self.frontier.add(
                    url=canonical,
                    depth=0,
                    score=0.0,
                    page_type="seed"
                )
                self._url_depth[canonical] = 0
                self._url_referrer[canonical] = None
                logger.info(f"Added seed: {canonical}")
        
        # Save initial frontier state
        self.frontier.persist()
        
        # Save initial service stats
        self._persist_service_stats()
        
        logger.info(f"Crawler initialized. Frontier size: {self.frontier.size()}, Already fetched: {self.frontier.fetched_count()}")
    
    async def run(self) -> None:
        """Main crawler loop - pop, fetch, store, extract, enqueue."""
        if self._running:
            logger.warning("Crawler already running")
            return
        
        self._running = True
        logger.info("Starting unified crawler main loop...")
        
        try:
            while not self._stop:
                # Check if frontier is empty
                if self.frontier.is_empty():
                    logger.info("Frontier empty - crawler waiting (or done)...")
                    await asyncio.sleep(5)
                    
                    # Check again after wait
                    if self.frontier.is_empty():
                        logger.info("Frontier still empty after wait - stopping crawler")
                        break
                    continue
                
                # Pop next URL
                url = self.frontier.pop()
                if not url:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process URL (fetch + store + extract + enqueue)
                await self._process_url(url)
                
                self._request_counter += 1
                
                per_request_sleep = random.uniform(
                    self.config.sleep.per_request_min,
                    self.config.sleep.per_request_max
                )
                logger.info(f"Sleeping for {per_request_sleep:.2f}s after request...")
                await asyncio.sleep(per_request_sleep)
                
                if self._request_counter % self.config.sleep.batch_size == 0:
                    batch_pause = random.uniform(
                        self.config.sleep.batch_pause_min,
                        self.config.sleep.batch_pause_max
                    )
                    logger.info(f"Batch pause ({self._request_counter} requests) - sleeping for {batch_pause:.2f}s...")
                    await asyncio.sleep(batch_pause)
                
                if time.time() - self._last_frontier_save > self._frontier_save_interval:
                    self.frontier.persist()
                    self._last_frontier_save = time.time()
                    logger.info(f"Periodic frontier save - remaining URLs: {self.frontier.size()}")
                
                # Periodic service stats persistence
                if time.time() - self._last_stats_save > self._stats_save_interval:
                    self._persist_service_stats()
                    self._last_stats_save = time.time()
        
        except asyncio.CancelledError:
            logger.info("Crawler cancelled")
        except Exception as e:
            logger.error(f"Error in crawler main loop: {e}", exc_info=True)
        finally:
            self._running = False
            logger.info("Crawler main loop stopped")
    
    async def _process_url(self, url: str):
        logger.info(f"Processing: {url}")
        
        depth = self._url_depth.get(url, 0)
        referrer = self._url_referrer.get(url)
        
        page_type = self.policy.classify(url)
        
        if self.frontier.is_fetched(url):
            logger.debug(f"URL already fetched (skipping): {url}")
            self.stats["already_fetched"] += 1
            return
        
        result = await self.fetcher.fetch_and_store(url)
        self.frontier.mark_fetched(url)
        
        if not result.get('ok'):
            logger.warning(f"Failed to fetch {url}: {result.get('error')}")
            self.stats["fetch_errors"] += 1
            
            metadata_record = self.metadata_writer.build_record(
                url=url,
                depth=depth,
                page_type=page_type or "unknown",
                referrer=referrer,
                http_status=result.get('status', 0),
                content_sha256="",
                stored_path="",
                content_bytes=0,
                fetch_latency_ms=result.get('fetch_latency_ms', 0.0),
                retries=result.get('retries', 0),
                extra_metadata={
                    'error': result.get('error'),
                    'user_agent': result.get('user_agent'),
                    'request_headers': result.get('request_headers')
                }
            )
            self.metadata_writer.write(metadata_record)
            return
        
        self.stats["urls_fetched"] += 1
        self.stats["urls_stored"] += 1
        
        logger.info(f"  ✓ Fetched and stored: {result['stored_path']} ({result['content_bytes']} bytes)")
        
        metadata_record = self.metadata_writer.build_record(
            url=url,
            depth=depth,
            page_type=page_type or "unknown",
            referrer=referrer,
            http_status=result['status'],
            content_sha256=result['content_sha256'],
            stored_path=result['stored_path'],
            content_bytes=result['content_bytes'],
            content_type=result.get('content_type', ''),
            encoding=result.get('encoding', ''),
            etag=result.get('etag', ''),
            last_modified=result.get('last_modified', ''),
            fetch_latency_ms=result['fetch_latency_ms'],
            retries=result['retries'],
            extra_metadata={
                'user_agent': result.get('user_agent'),
                'request_headers': result.get('request_headers')
            }
        )
        self.metadata_writer.write(metadata_record)
        
        # Extract links for further crawling
        # Use HTML from fetch result (more efficient than reading from disk)
        html = result.get('html', '')
        
        if html:
            extracted_links = self.extractor.extract(html, url)
            self.stats["links_extracted"] += len(extracted_links)
            
            logger.info(f"  → Extracted {len(extracted_links)} links")
            
            # Enqueue extracted links
            child_depth = depth + 1
            
            if child_depth > self.config.caps.max_depth:
                logger.debug(f"  ⚠ Depth limit reached for children of {url}")
            else:
                for link in extracted_links:
                    await self._enqueue_url(link, url, child_depth)
        else:
            logger.warning(f"No HTML content returned for link extraction: {url}")
    
    async def _enqueue_url(self, url: str, referrer: str, depth: int):
        canonical = canonicalize(url)
        if referrer and canonical == referrer:
            return
        
        if self.frontier.is_fetched(canonical):
            return
        
        if self.frontier.contains(canonical):
            return
        
        policy_result = await self.policy.gate(canonical)
        if not policy_result["ok"]:
            self.stats["policy_denied"] += 1
            logger.debug(f"Policy denied {canonical}: {policy_result['reason']}")
            return
        canonical_url = policy_result.get("canonical_url", canonical)
        if canonical_url != canonical:
            canonical = canonical_url
            if referrer and canonical == referrer:
                return
            if self.frontier.is_fetched(canonical) or self.frontier.contains(canonical):
                return
        
        if not self._check_caps(canonical, policy_result.get("page_type")):
            self.stats["cap_exceeded"] += 1
            logger.debug(f"Cap exceeded for {canonical}")
            return
        
        self.frontier.add(
            url=canonical,
            depth=depth,
            score=float(depth),  # Use depth as score for BFS
            page_type=policy_result.get("page_type"),
            referrer=referrer
        )
        
        self._url_depth[canonical] = depth
        self._url_referrer[canonical] = referrer
        
        self.stats["urls_enqueued"] += 1
        logger.debug(f"Enqueued: {canonical} (depth={depth})")
    
    def _check_caps(self, url: str, page_type: Optional[str]) -> bool:

        repo_info = extract_repo_info(url)
        if not repo_info:
            return True
        
        owner, repo = repo_info
        repo_key = f"{owner}/{repo}"
        
        if page_type == "issues":
            if self._repo_issues[repo_key] >= self.config.caps.per_repo_max_issues:
                return False
            self._repo_issues[repo_key] += 1
        
        elif page_type == "pull":
            if self._repo_prs[repo_key] >= self.config.caps.per_repo_max_prs:
                return False
            self._repo_prs[repo_key] += 1
        
        else:
            if self._repo_pages[repo_key] >= self.config.caps.per_repo_max_pages:
                return False
            self._repo_pages[repo_key] += 1
        
        return True
    
    async def stop(self) -> None:
        logger.info("Stopping crawler...")
        self._stop = True
        
        while self._running:
            await asyncio.sleep(0.1)
        
        logger.info("Cleaning up components...")
        
        if self.frontier:
            self.frontier.close()
        
        if self.policy:
            self.policy.close()
        
        if self.fetcher:
            await self.fetcher.close()
        
        if self.metadata_writer:
            self.metadata_writer.close()

        # Persist final service stats
        self._persist_service_stats()

        # Calculate final metrics for display
        html_files = self._calculate_html_files_count()
        storage_bytes = self._calculate_html_storage_bytes()
        runtime_seconds = self._get_runtime_seconds()
        acceptance_rate = self._get_acceptance_rate()

        logger.info("=== Crawler Statistics ===")
        logger.info("--- Core Metrics ---")
        for key, value in self.stats.items():
            logger.info(f"  {key}: {value}")
        
        logger.info("--- Storage Metrics ---")
        logger.info(f"  html_files_count: {html_files}")
        logger.info(f"  html_storage_bytes: {storage_bytes}")
        logger.info(f"  html_storage_size: {self._format_bytes(storage_bytes)}")
        
        logger.info("--- Runtime Metrics ---")
        logger.info(f"  runtime_seconds: {runtime_seconds:.2f}")
        logger.info(f"  runtime_formatted: {self._format_runtime(runtime_seconds)}")
        
        logger.info("--- Derived Metrics ---")
        logger.info(f"  acceptance_rate: {acceptance_rate:.2f}%")
        
        total_evaluated = self.stats["urls_enqueued"] + self.stats["policy_denied"]
        if total_evaluated > 0:
            logger.info(f"  total_urls_evaluated: {total_evaluated}")
            logger.info(f"  urls_accepted: {self.stats['urls_enqueued']} ({acceptance_rate:.2f}%)")
            logger.info(f"  urls_rejected: {self.stats['policy_denied']} ({100 - acceptance_rate:.2f}%)")

        logger.info("Crawler stopped successfully")
    
    def _format_runtime(self, seconds: float) -> str:
        """Format runtime seconds into human-readable string.
        
        Args:
            seconds: Runtime in seconds
            
        Returns:
            Formatted string (e.g., "2h 15m 30s")
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        parts = []
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")
        if secs > 0 or not parts:
            parts.append(f"{secs}s")
        
        return " ".join(parts)
