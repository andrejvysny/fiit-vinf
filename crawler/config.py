"""Configuration management for GitHub Crawler.

Loads and validates crawler configuration from YAML files.
Provides strict typing and validation of all config parameters.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Optional
import yaml
import os


@dataclass
class RobotsConfig:
    """Robots.txt configuration."""
    user_agent: str
    cache_ttl_sec: int = 86400  # 24 hours default

    def __post_init__(self):
        if not self.user_agent:
            raise ValueError("robots.user_agent cannot be empty")
        if self.cache_ttl_sec < 60:
            raise ValueError("robots.cache_ttl_sec must be at least 60 seconds")


@dataclass
class ScopeConfig:
    """URL scope configuration."""
    allowed_hosts: List[str]
    denied_subdomains: List[str] = field(default_factory=list)
    allow_patterns: List[str] = field(default_factory=list)
    deny_patterns: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=lambda: ["text/html"])

    def __post_init__(self):
        if not self.allowed_hosts:
            raise ValueError("scope.allowed_hosts cannot be empty")
        # Ensure github.com is the only allowed host
        if self.allowed_hosts != ["github.com"]:
            raise ValueError("Only github.com is allowed as host")


@dataclass
class LimitsConfig:
    """Rate limiting and timeout configuration."""
    global_concurrency: int = 2
    per_host_concurrency: int = 2
    req_per_sec: float = 1.0
    connect_timeout_ms: int = 4000
    read_timeout_ms: int = 12000
    total_timeout_ms: int = 20000
    max_retries: int = 3
    backoff_base_ms: int = 500
    backoff_cap_ms: int = 8000

    def __post_init__(self):
        if self.global_concurrency < 1:
            raise ValueError("limits.global_concurrency must be at least 1")
        if self.req_per_sec <= 0:
            raise ValueError("limits.req_per_sec must be positive")
        if self.max_retries < 0:
            raise ValueError("limits.max_retries cannot be negative")


@dataclass
class CapsConfig:
    """Crawl depth and per-repository limits."""
    per_repo_max_pages: int = 30
    per_repo_max_issues: int = 10
    per_repo_max_prs: int = 10
    max_depth: int = 4

    def __post_init__(self):
        if self.max_depth < 1:
            raise ValueError("caps.max_depth must be at least 1")
        if self.per_repo_max_pages < 1:
            raise ValueError("caps.per_repo_max_pages must be at least 1")


@dataclass
class FrontierConfig:
    """Frontier storage configuration."""
    db_path: str
    bloom_path: str
    bloom_capacity: int = 50_000_000
    bloom_error_rate: float = 0.001

    def __post_init__(self):
        if not self.db_path:
            raise ValueError("frontier.db_path cannot be empty")
        if not self.bloom_path:
            raise ValueError("frontier.bloom_path cannot be empty")
        if self.bloom_capacity < 1000:
            raise ValueError("frontier.bloom_capacity too small")
        if not (0 < self.bloom_error_rate < 0.1):
            raise ValueError("frontier.bloom_error_rate must be between 0 and 0.1")


@dataclass
class SpoolConfig:
    """Discoveries output configuration."""
    discoveries_dir: str
    rotate_every_mb: int = 100
    backpressure_limit_gb: float = 10.0

    def __post_init__(self):
        if not self.discoveries_dir:
            raise ValueError("spool.discoveries_dir cannot be empty")
        if self.rotate_every_mb < 1:
            raise ValueError("spool.rotate_every_mb must be at least 1")
        if self.backpressure_limit_gb <= 0:
            raise ValueError("spool.backpressure_limit_gb must be positive")


@dataclass
class MetricsConfig:
    """Metrics output configuration."""
    csv_path: str
    flush_interval_sec: int = 10

    def __post_init__(self):
        if not self.csv_path:
            raise ValueError("metrics.csv_path cannot be empty")
        if self.flush_interval_sec < 1:
            raise ValueError("metrics.flush_interval_sec must be at least 1")


@dataclass
class TrajectoryConfig:
    """Trajectory (edges) output configuration."""
    csv_path: str

    def __post_init__(self):
        if not self.csv_path:
            raise ValueError("trajectory.csv_path cannot be empty")


@dataclass
class CrawlerConfig:
    """Main crawler configuration."""
    run_id: str
    workspace: str
    user_agent: str
    accept_language: str = "en"
    accept_encoding: str = "br, gzip"

    # Nested configs
    robots: RobotsConfig = None
    scope: ScopeConfig = None
    limits: LimitsConfig = None
    caps: CapsConfig = None
    frontier: FrontierConfig = None
    spool: SpoolConfig = None
    metrics: MetricsConfig = None
    trajectory: TrajectoryConfig = None

    def __post_init__(self):
        """Validate configuration and set defaults."""
        if not self.run_id:
            raise ValueError("run_id cannot be empty")
        if not self.workspace:
            raise ValueError("workspace cannot be empty")
        if not self.user_agent or "+http" not in self.user_agent:
            raise ValueError("user_agent must include contact URL (+https://...)")

        # Log compliance links at startup
        print("\n" + "="*70)
        print("GitHub Crawler - Compliance Links")
        print("="*70)
        print(f"User-Agent: {self.user_agent}")
        print(f"Robots UA: {self.robots.user_agent if self.robots else 'Not set'}")
        print("\nCompliance References:")
        print("  - Robots.txt: https://github.com/robots.txt")
        print("  - Acceptable Use: https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies")
        print("  - Terms of Service: https://docs.github.com/site-policy/github-terms/github-terms-of-service")
        print("="*70 + "\n")

    @classmethod
    def from_yaml(cls, path: str) -> "CrawlerConfig":
        """Load configuration from YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)

        # Parse nested configs
        config = cls(
            run_id=raw.get("run_id", ""),
            workspace=raw.get("workspace", ""),
            user_agent=raw.get("user_agent", ""),
            accept_language=raw.get("accept_language", "en"),
            accept_encoding=raw.get("accept_encoding", "br, gzip"),
        )

        # Parse robots config
        if "robots" in raw:
            config.robots = RobotsConfig(**raw["robots"])
        else:
            raise ValueError("robots configuration is required")

        # Parse scope config
        if "scope" in raw:
            config.scope = ScopeConfig(**raw["scope"])
        else:
            raise ValueError("scope configuration is required")

        # Parse limits config
        if "limits" in raw:
            config.limits = LimitsConfig(**raw["limits"])
        else:
            config.limits = LimitsConfig()  # Use defaults

        # Parse caps config
        if "caps" in raw:
            config.caps = CapsConfig(**raw["caps"])
        else:
            config.caps = CapsConfig()  # Use defaults

        # Expand workspace paths for storage configs
        workspace = Path(config.workspace).expanduser().absolute()

        # Parse frontier config
        if "frontier" in raw:
            f = raw["frontier"]
            config.frontier = FrontierConfig(
                db_path=str(workspace / "state" / f.get("db_name", "frontier.lmdb")),
                bloom_path=str(workspace / "state" / f.get("bloom_name", "seen.bloom")),
                bloom_capacity=f.get("bloom_capacity", 50_000_000),
                bloom_error_rate=f.get("bloom_error_rate", 0.001)
            )
        else:
            config.frontier = FrontierConfig(
                db_path=str(workspace / "state" / "frontier.lmdb"),
                bloom_path=str(workspace / "state" / "seen.bloom")
            )

        # Parse spool config
        if "spool" in raw:
            s = raw["spool"]
            config.spool = SpoolConfig(
                discoveries_dir=str(workspace / "spool" / "discoveries"),
                rotate_every_mb=s.get("rotate_every_mb", 100),
                backpressure_limit_gb=s.get("backpressure_limit_gb", 10.0)
            )
        else:
            config.spool = SpoolConfig(
                discoveries_dir=str(workspace / "spool" / "discoveries")
            )

        # Parse metrics config
        if "metrics" in raw:
            m = raw["metrics"]
            config.metrics = MetricsConfig(
                csv_path=str(workspace / "logs" / m.get("filename", "crawler_metrics.csv")),
                flush_interval_sec=m.get("flush_interval_sec", 10)
            )
        else:
            config.metrics = MetricsConfig(
                csv_path=str(workspace / "logs" / "crawler_metrics.csv")
            )

        # Parse trajectory config
        if "trajectory" in raw:
            t = raw["trajectory"]
            config.trajectory = TrajectoryConfig(
                csv_path=str(workspace / "logs" / t.get("filename", "edges.csv"))
            )
        else:
            config.trajectory = TrajectoryConfig(
                csv_path=str(workspace / "logs" / "edges.csv")
            )

        return config

    def get_workspace_path(self) -> Path:
        """Get workspace as Path object."""
        return Path(self.workspace).expanduser().absolute()
