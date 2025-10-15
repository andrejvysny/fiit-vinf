
from dataclasses import dataclass, field
from pathlib import Path
from typing import List
import yaml


@dataclass
class RobotsConfig:
    user_agent: str
    cache_ttl_sec: int = 86400
    def __post_init__(self):
        if not self.user_agent:
            raise ValueError("robots.user_agent cannot be empty")


@dataclass
class ScopeConfig:
    allowed_hosts: List[str]
    denied_subdomains: List[str] = field(default_factory=list)
    allow_patterns: List[str] = field(default_factory=list)
    deny_patterns: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=lambda: ["text/html"])

    def __post_init__(self):
        if not self.allowed_hosts:
            raise ValueError("scope.allowed_hosts cannot be empty")


@dataclass
class LimitsConfig:
    global_concurrency: int = 2
    per_host_concurrency: int = 2
    req_per_sec: float = 1.0
    connect_timeout_ms: int = 4000
    read_timeout_ms: int = 15000
    total_timeout_ms: int = 25000
    max_retries: int = 3
    backoff_base_ms: int = 500
    backoff_cap_ms: int = 8000


@dataclass
class CapsConfig:
    per_repo_max_pages: int = 30
    per_repo_max_issues: int = 10
    per_repo_max_prs: int = 10
    max_depth: int = 4


@dataclass
class StorageConfig:
    frontier_file: str
    fetched_urls_file: str
    robots_cache_file: str
    metadata_file: str
    html_store_root: str
    html_compress: bool = False
    html_permissions: int = 0o644


@dataclass
class LogsConfig:
    log_file: str = "logs/crawler.log"
    log_level: str = "INFO"


@dataclass
class CrawlerScraperConfig:
    run_id: str
    workspace: str
    user_agent: str
    accept_language: str = "en"
    accept_encoding: str = "br, gzip"
    
    robots: RobotsConfig = None
    scope: ScopeConfig = None
    limits: LimitsConfig = None
    caps: CapsConfig = None
    storage: StorageConfig = None
    logs: LogsConfig = None

    def __post_init__(self):
        if not self.user_agent:
            raise ValueError("user_agent cannot be empty")
        if not self.workspace:
            raise ValueError("workspace cannot be empty")

    def get_workspace_path(self) -> Path:
        return Path(self.workspace).resolve()

    @classmethod
    def from_yaml(cls, config_path: str) -> 'CrawlerScraperConfig':
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)

        robots_cfg = RobotsConfig(**data.get('robots', {}))
        scope_cfg = ScopeConfig(**data.get('scope', {}))
        limits_cfg = LimitsConfig(**data.get('limits', {}))
        caps_cfg = CapsConfig(**data.get('caps', {}))
        storage_cfg = StorageConfig(**data.get('storage', {}))
        logs_cfg = LogsConfig(**data.get('logs', {}))

        config = cls(
            run_id=data['run_id'],
            workspace=data['workspace'],
            user_agent=data['user_agent'],
            accept_language=data.get('accept_language', 'en'),
            accept_encoding=data.get('accept_encoding', 'br, gzip'),
            robots=robots_cfg,
            scope=scope_cfg,
            limits=limits_cfg,
            caps=caps_cfg,
            storage=storage_cfg,
            logs=logs_cfg
        )

        return config
