"""
Main Scraper Service - orchestrates all components
"""

import asyncio
import time
import json
from typing import Optional, Dict, Any
from pathlib import Path

from .config import ScraperConfig
from .util.signals import SignalHandler
from .util.bytes import dir_size_gb, format_bytes
from .control.flags import ControlFlags
from .io.spool import SpoolReader
from .io.storage import HtmlStore
from .io.csv_writer import PagesCsv
from .io.metrics import Metrics
from .index.page_index import PageIndex
from .net.fetcher import Fetcher
from .net.proxy_client import ProxyClient, NoProxyClient
from .util.limits import RateLimiter


class ScraperService:
    """Main scraper service orchestrator"""

    def __init__(self, config: ScraperConfig, signal_handler: Optional[SignalHandler] = None,
                 proxy_client: Optional[ProxyClient] = None):
        self.config = config
        self.signal_handler = signal_handler
        self.proxy_client = proxy_client or NoProxyClient()
        self.control_flags = ControlFlags(config.workspace)

        # Store config path for reloading
        self.config_path = None

        # Component instances (initialized in setup)
        self.spool_reader = None
        self.html_store = None
        self.pages_csv = None
        self.metrics = None
        self.page_index = None
        self.fetcher = None
        self.limiter = None

        # Runtime state
        self.start_time = None
        self.pages_processed = 0
        self.last_url = None
        self.last_status = None
        self.running = False

        # Setup signal callbacks
        if self.signal_handler:
            self.signal_handler.on_reload(self._reload_config)
            self.signal_handler.on_status(self._get_status)

    def _reload_config(self):
        """Reload configuration (SIGHUP handler)"""
        if self.config_path:
            try:
                self.config.reload(self.config_path)
                print(f"Configuration reloaded from {self.config_path}")
            except Exception as e:
                print(f"Failed to reload config: {e}")
        else:
            print("Config path not set - cannot reload")

    def _get_status(self) -> dict:
        """Get current status (SIGUSR1 handler)"""
        status = {
            "pages_processed": self.pages_processed,
            "last_url": self.last_url or "None",
            "last_status": self.last_status or "None",
            "uptime": f"{time.time() - self.start_time:.0f}s" if self.start_time else "Not started"
        }

        # Add component stats if available
        if self.spool_reader:
            status["spool"] = self.spool_reader.get_stats()
        if self.metrics:
            status["metrics"] = self.metrics.get_stats()
        if self.page_index:
            status["index"] = self.page_index.get_stats()
        if self.fetcher:
            status["fetcher"] = self.fetcher.get_stats()

        return status

    async def setup(self):
        """Initialize all components"""
        print("Initializing scraper components...")

        # Initialize components
        self.spool_reader = SpoolReader(
            self.config.spool.dir,
            self.config.bookmark.path
        )

        self.html_store = HtmlStore(
            self.config.store.root,
            self.config.store.compress,
            self.config.store.permissions
        )

        self.pages_csv = PagesCsv(self.config.pages_csv.path)

        self.metrics = Metrics(
            self.config.metrics.csv_path,
            self.config.metrics.flush_interval_sec
        )

        self.page_index = PageIndex(self.config.index.db_path)

        self.limiter = RateLimiter(
            self.config.limits.global_concurrency,
            self.config.limits.req_per_sec
        )

        self.fetcher = Fetcher(
            self.config,
            self.proxy_client,
            self.limiter
        )

        self.start_time = time.time()
        self.running = True
        print("Components initialized successfully")

    async def run(self):
        """Main scraper loop"""
        await self.setup()

        print("\nScraper service starting...")
        print(f"Reading from spool: {self.config.spool.dir}")
        print(f"Storing HTML in: {self.config.store.root}")
        print(f"Writing CSV to: {self.config.pages_csv.path}")
        print("\nWaiting for discoveries from spool...")

        # Create shutdown task if signal handler available
        shutdown_task = None
        if self.signal_handler:
            shutdown_task = asyncio.create_task(self.signal_handler.wait_for_shutdown())

        try:
            async for item in self.spool_reader.iter_items():
                # Check for shutdown
                if shutdown_task and shutdown_task.done():
                    print("\nShutdown signal received")
                    break

                # Check control flags
                await self.control_flags.wait_while_paused()
                if await self.control_flags.check_stop():
                    break

                # Check backpressure
                spool_size_gb = dir_size_gb(self.config.spool.dir)
                if spool_size_gb > self.config.spool.max_backlog_gb:
                    print(f"\nBACKPRESSURE: Spool size ({spool_size_gb:.2f} GB) exceeds threshold ({self.config.spool.max_backlog_gb} GB)")
                    print("Pausing consumption for 10 seconds...")
                    await asyncio.sleep(10)
                    continue

                # Process discovery item
                await self.process_item(item)

                # Flush metrics periodically
                self.metrics.maybe_flush()

        except Exception as e:
            print(f"\nERROR in main loop: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def process_item(self, item: Dict[str, Any]):
        """Process a single discovery item"""
        url = item.get("url")
        if not url:
            print(f"WARNING: Item missing URL: {item}")
            return

        self.last_url = url
        page_type = item.get("page_type")
        depth = item.get("depth")
        referrer = item.get("referrer")
        metadata = item.get("metadata", {})
        timestamp = int(time.time())

        print(f"\nProcessing: {url}")

        # Check index for previous data (for conditional requests)
        prev = self.page_index.get(url)
        if prev:
            print(f"  Found in index - ETag: {prev.get('etag', 'None')}")

        # Fetch the page
        result = await self.fetcher.fetch_html(url, prev)
        self.last_status = result.get("status")

        if not result.get("ok"):
            # Failed to fetch
            if result.get("not_html"):
                print(f"  ✗ Not HTML: {result.get('content_type')}")
                self.metrics.inc("not_html")
            else:
                print(f"  ✗ Failed: {result.get('error')}")
                self.metrics.inc("fetch_fail")
                if result.get("status"):
                    status_bucket = f"status_{result['status'] // 100}xx"
                    self.metrics.inc(status_bucket)
            return

        # Successful fetch
        status = result["status"]
        headers = result.get("headers", {})

        # Update status metrics
        status_bucket = f"status_{status // 100}xx"
        self.metrics.inc(status_bucket)
        self.metrics.inc("pages_fetched")

        # Extract headers
        content_type = headers.get("Content-Type", "") or headers.get("content-type", "")
        encoding = headers.get("Content-Encoding", "") or headers.get("content-encoding", "")
        etag = headers.get("ETag", "") or headers.get("etag", "")
        last_modified = headers.get("Last-Modified", "") or headers.get("last-modified", "")

        # Handle 304 Not Modified
        if result.get("not_modified"):
            print(f"  → 304 Not Modified (using cached)")
            self.metrics.inc("not_modified_304")

            # Use previous data
            row = {
                "timestamp": timestamp,
                "url": url,
                "page_type": page_type,
                "depth": depth,
                "referrer": referrer,
                "http_status": status,
                "content_type": content_type,
                "encoding": encoding,
                "content_sha256": prev.get("sha256", "") if prev else "",
                "content_bytes": 0,
                "stored_path": prev.get("stored_path", "") if prev else "",
                "etag": etag or (prev.get("etag", "") if prev else ""),
                "last_modified": last_modified or (prev.get("last_modified", "") if prev else ""),
                "fetch_latency_ms": int(result.get("latency_ms", 0)),
                "retries": result.get("retries", 0),
                "proxy_id": result.get("proxy_id", ""),
                "metadata": metadata
            }
            self.pages_csv.append(row)

        else:
            # New content - store it
            html_bytes = result.get("html_bytes", b"")
            if not html_bytes:
                print(f"  ✗ Empty response")
                return

            # Save HTML
            saved = self.html_store.save(html_bytes)
            print(f"  ✓ Saved: {format_bytes(saved['bytes'])}")
            if saved.get("compressed_bytes"):
                print(f"    Compressed: {format_bytes(saved['compressed_bytes'])} ({saved.get('compression_ratio', 'N/A')})")
            if saved.get("already_exists"):
                print(f"    (Already existed - deduped)")

            self.metrics.inc("pages_saved")
            self.metrics.inc("bytes_saved", saved["bytes"])

            # Update index
            index_record = {
                "url": url,
                "sha256": saved["sha256"],
                "stored_path": saved["stored_path"],
                "etag": etag,
                "last_modified": last_modified,
                "timestamp": timestamp
            }
            self.page_index.put(index_record)

            # Write CSV row
            row = {
                "timestamp": timestamp,
                "url": url,
                "page_type": page_type,
                "depth": depth,
                "referrer": referrer,
                "http_status": status,
                "content_type": content_type,
                "encoding": encoding,
                "content_sha256": saved["sha256"],
                "content_bytes": saved["bytes"],
                "stored_path": saved["stored_path"],
                "etag": etag,
                "last_modified": last_modified,
                "fetch_latency_ms": int(result.get("latency_ms", 0)),
                "retries": result.get("retries", 0),
                "proxy_id": result.get("proxy_id", ""),
                "metadata": metadata
            }
            self.pages_csv.append(row)

        # Update retries metric
        if result.get("retries", 0) > 0:
            self.metrics.inc("retries_total", result["retries"])

        self.pages_processed += 1

        # Print progress every 10 pages
        if self.pages_processed % 10 == 0:
            print(f"\nProgress: {self.pages_processed} pages processed")
            print(self.metrics.get_summary())

    async def cleanup(self):
        """Clean up resources"""
        print("\nCleaning up resources...")
        self.running = False

        # Save final bookmark
        if self.spool_reader:
            self.spool_reader._save_bookmark(force=True)

        # Close components
        if self.pages_csv:
            self.pages_csv.close()
            print(f"  CSV closed: {self.pages_csv.rows_written} rows written")

        if self.metrics:
            self.metrics.flush()
            print(f"  Metrics flushed")

        if self.page_index:
            self.page_index.sync()
            self.page_index.close()
            print(f"  Index closed: {self.page_index.size()} entries")

        if self.fetcher:
            await self.fetcher.close()
            print(f"  Fetcher closed")

        # Print final statistics
        if self.start_time:
            runtime = time.time() - self.start_time
            print(f"\nFinal statistics:")
            print(f"  Runtime: {runtime:.1f}s")
            print(f"  Pages processed: {self.pages_processed}")
            if self.pages_processed > 0:
                print(f"  Average: {runtime/self.pages_processed:.2f}s per page")

        print("\nCleanup complete")

    async def replay(self):
        """Replay mode - rebuild CSV from stored HTML"""
        print("REPLAY MODE - Rebuilding pages CSV from stored HTML")
        print(f"Store: {self.config.store.root}")
        print(f"Output: {self.config.pages_csv.path}")
        print()

        # Initialize components needed for replay
        self.html_store = HtmlStore(
            self.config.store.root,
            self.config.store.compress,
            self.config.store.permissions
        )

        self.pages_csv = PagesCsv(self.config.pages_csv.path)
        self.page_index = PageIndex(self.config.index.db_path)

        # Iterate through all entries in index
        count = 0
        for url, record in self.page_index.iterate():
            sha256 = record.get("sha256")
            stored_path = record.get("stored_path")

            if not sha256 or not stored_path:
                continue

            # Check if file exists
            if not Path(stored_path).exists():
                print(f"WARNING: File not found: {stored_path}")
                continue

            # Read HTML to get size
            try:
                html_bytes = self.html_store.read(sha256)
                content_bytes = len(html_bytes)
            except Exception as e:
                print(f"WARNING: Failed to read {sha256}: {e}")
                content_bytes = 0

            # Create CSV row from index data
            row = {
                "timestamp": record.get("timestamp", ""),
                "url": url,
                "page_type": "",  # Not stored in index
                "depth": "",       # Not stored in index
                "referrer": "",    # Not stored in index
                "http_status": 200,  # Assume successful
                "content_type": "text/html",
                "encoding": "",
                "content_sha256": sha256,
                "content_bytes": content_bytes,
                "stored_path": stored_path,
                "etag": record.get("etag", ""),
                "last_modified": record.get("last_modified", ""),
                "fetch_latency_ms": "",
                "retries": 0,
                "proxy_id": "",
                "metadata": ""
            }

            self.pages_csv.append(row)
            count += 1

            if count % 100 == 0:
                print(f"  Processed {count} pages...")

        # Cleanup
        self.pages_csv.close()
        self.page_index.close()

        print(f"\nReplay complete: {count} pages written to CSV")