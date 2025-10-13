"""
Scraper configuration management with YAML parsing and validation
"""

import os
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class LimitsConfig:
    """Rate limiting and timeout configuration"""
    global_concurrency: int = 2
    req_per_sec: float = 1.0
    connect_timeout_ms: int = 4000
    read_timeout_ms: int = 15000
    total_timeout_ms: int = 25000
    max_retries: int = 3
    backoff_base_ms: int = 500
    backoff_cap_ms: int = 8000

    def __post_init__(self):
        if self.global_concurrency < 1:
            raise ValueError(f"global_concurrency must be >= 1, got {self.global_concurrency}")
        if self.req_per_sec <= 0:
            raise ValueError(f"req_per_sec must be > 0, got {self.req_per_sec}")
        if self.connect_timeout_ms < 100:
            raise ValueError(f"connect_timeout_ms too low: {self.connect_timeout_ms}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")


@dataclass
class SpoolConfig:
    """Discovery spool configuration"""
    dir: str = "workspace/spool/discoveries"
    max_backlog_gb: float = 10.0

    def __post_init__(self):
        if self.max_backlog_gb <= 0:
            raise ValueError(f"max_backlog_gb must be > 0, got {self.max_backlog_gb}")


@dataclass
class StoreConfig:
    """HTML storage configuration"""
    root: str = "workspace/store/html"
    compress: bool = False
    permissions: int = 0o644

    def __post_init__(self):
        if not self.root:
            raise ValueError("store.root is required")


@dataclass
class PagesCsvConfig:
    """Pages CSV output configuration"""
    path: str = "workspace/logs/pages-{run_id}.csv"


@dataclass
class MetricsConfig:
    """Metrics CSV configuration"""
    csv_path: str = "workspace/logs/scraper_metrics-{run_id}.csv"
    flush_interval_sec: int = 10

    def __post_init__(self):
        if self.flush_interval_sec < 1:
            raise ValueError(f"flush_interval_sec must be >= 1, got {self.flush_interval_sec}")


@dataclass
class IndexConfig:
    """File-based page index configuration"""
    db_path: str = "workspace/state/page_index.jsonl"


@dataclass
class BookmarkConfig:
    """Spool bookmark configuration"""
    path: str = "workspace/state/spool_bookmark.json"


@dataclass
class ScraperConfig:
    """Main scraper configuration"""
    run_id: str
    workspace: str
    user_agent: str
    accept_language: str = "en"
    accept_encoding: str = "br, gzip"

    limits: LimitsConfig = field(default_factory=LimitsConfig)
    spool: SpoolConfig = field(default_factory=SpoolConfig)
    store: StoreConfig = field(default_factory=StoreConfig)
    pages_csv: PagesCsvConfig = field(default_factory=PagesCsvConfig)
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    index: IndexConfig = field(default_factory=IndexConfig)
    bookmark: BookmarkConfig = field(default_factory=BookmarkConfig)

    def __post_init__(self):
        """Validate configuration and expand paths"""
        if not self.run_id:
            raise ValueError("run_id is required")
        if not self.workspace:
            raise ValueError("workspace is required")
        if not self.user_agent:
            raise ValueError("user_agent is required")

        # Check user agent contains contact info
        if "+" not in self.user_agent:
            print("WARNING: user_agent should contain contact info (e.g., '+contact@example.com')")

        # Expand workspace paths
        self._expand_paths()

        # Print compliance banner
        self._print_compliance_banner()

    def _expand_paths(self):
        """Expand workspace and run_id placeholders in paths"""
        workspace = os.path.expanduser(self.workspace)

        # Expand run_id placeholders
        self.pages_csv.path = self.pages_csv.path.replace("{run_id}", self.run_id)
        self.metrics.csv_path = self.metrics.csv_path.replace("{run_id}", self.run_id)

        # Make paths absolute if relative to workspace
        if not os.path.isabs(self.spool.dir):
            self.spool.dir = os.path.join(workspace, self.spool.dir)
        if not os.path.isabs(self.store.root):
            self.store.root = os.path.join(workspace, self.store.root)
        if not os.path.isabs(self.pages_csv.path):
            self.pages_csv.path = os.path.join(workspace, self.pages_csv.path)
        if not os.path.isabs(self.metrics.csv_path):
            self.metrics.csv_path = os.path.join(workspace, self.metrics.csv_path)
        if not os.path.isabs(self.index.db_path):
            self.index.db_path = os.path.join(workspace, self.index.db_path)
        if not os.path.isabs(self.bookmark.path):
            self.bookmark.path = os.path.join(workspace, self.bookmark.path)

    def _print_compliance_banner(self):
        """Print compliance information at startup"""
        print("=" * 70)
        print("VI2025 SCRAPER - GitHub HTML Content Scraper")
        print("=" * 70)
        print(f"User-Agent: {self.user_agent}")
        print(f"Run ID: {self.run_id}")
        print()
        print("COMPLIANCE LINKS:")
        print("  - Robots.txt: https://github.com/robots.txt")
        print("  - Acceptable Use: https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies")
        print("  - Terms of Service: https://docs.github.com/site-policy/github-terms/github-terms-of-service")
        print()
        print("NOTE: This scraper respects robots.txt and rate limits.")
        print("      Operating in conservative mode (2 concurrent, 1 req/sec)")
        print("=" * 70)
        print()

    @classmethod
    def from_yaml(cls, path: str) -> "ScraperConfig":
        """Load configuration from YAML file"""
        with open(path, "r") as f:
            data = yaml.safe_load(f)

        # Parse nested configs
        if "limits" in data:
            data["limits"] = LimitsConfig(**data["limits"])
        if "spool" in data:
            data["spool"] = SpoolConfig(**data["spool"])
        if "store" in data:
            store_raw = dict(data["store"])
            # Coerce permissions string like '0o644' to int
            if "permissions" in store_raw and isinstance(store_raw["permissions"], str):
                try:
                    store_raw["permissions"] = int(store_raw["permissions"], 0)
                except Exception:
                    # Fall back to default if parsing fails
                    store_raw["permissions"] = StoreConfig().permissions
            data["store"] = StoreConfig(**store_raw)
        if "pages_csv" in data:
            data["pages_csv"] = PagesCsvConfig(**data["pages_csv"])
        if "metrics" in data:
            data["metrics"] = MetricsConfig(**data["metrics"])
        if "index" in data:
            data["index"] = IndexConfig(**data["index"])
        if "bookmark" in data:
            data["bookmark"] = BookmarkConfig(**data["bookmark"])

        return cls(**data)

    def reload(self, path: str):
        """Reload configuration from YAML (for SIGHUP)"""
        new_config = self.from_yaml(path)

        # Update reloadable fields
        self.limits = new_config.limits
        self.store.compress = new_config.store.compress
        self.store.permissions = new_config.store.permissions
        self.spool.max_backlog_gb = new_config.spool.max_backlog_gb
        self.metrics.flush_interval_sec = new_config.metrics.flush_interval_sec

        print(f"Configuration reloaded from {path}")