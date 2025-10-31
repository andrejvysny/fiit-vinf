import argparse
import asyncio
import logging
from pathlib import Path
from typing import Optional

from .config import CrawlerScraperConfig
from .service import CrawlerScraperService


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _ensure_file(p: Path, content: Optional[str] = "") -> bool:
    if not p.exists():
        if p.parent and not p.parent.exists():
            _ensure_dir(p.parent)
        with open(p, "w", encoding="utf-8") as f:
            if content is not None:
                f.write(content)
        return True
    return False


def _attach_file_logging(log_path: Path, level: str) -> None:
    logger = logging.getLogger()

    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            try:
                if Path(getattr(h, "baseFilename", "")) == log_path:
                    return
            except Exception:
                continue

    _ensure_dir(log_path.parent)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def preflight_environment(config: CrawlerScraperConfig) -> None:
    ws = Path(config.workspace).resolve()

    _ensure_dir(ws)

    frontier_path = ws / Path(config.storage.frontier_file)
    fetched_urls_path = ws / Path(config.storage.fetched_urls_file)
    robots_cache_path = ws / Path(config.storage.robots_cache_file)
    metadata_path = ws / Path(config.storage.metadata_file)
    html_store_root = ws / Path(config.storage.html_store_root)
    logs_path = ws / Path(config.logs.log_file)

    for d in {
        frontier_path.parent,
        fetched_urls_path.parent,
        robots_cache_path.parent,
        metadata_path.parent,
        html_store_root,
        logs_path.parent,
    }:
        _ensure_dir(d)

    created_frontier = _ensure_file(frontier_path, "")
    created_fetched = _ensure_file(fetched_urls_path, "")
    created_robots = _ensure_file(robots_cache_path, "")
    created_metadata = _ensure_file(metadata_path, "")

    _attach_file_logging(logs_path, config.logs.log_level)

    logging.getLogger(__name__).info(
        "Preflight: workspace prepared at %s; created files -> frontier:%s fetched:%s robots:%s metadata:%s; logs -> %s",
        str(ws),
        created_frontier,
        created_fetched,
        created_robots,
        created_metadata,
        str(logs_path),
    )


async def run_crawler(config: CrawlerScraperConfig, seeds: list):
    service = CrawlerScraperService(config)

    try:
        await service.start(seeds)
        await service.run()

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:  # noqa: BLE001 - surface stack trace to user
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
    finally:
        await service.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="GitHub Crawler/Scraper")

    parser.add_argument(
        "--config",
        required=False,
        help="Path to unified config YAML file",
        default="config.yaml",
    )

    args = parser.parse_args()

    print(f"Loading configuration from: {args.config}")
    config = CrawlerScraperConfig.from_yaml(args.config)

    logging.basicConfig(
        level=getattr(logging, config.logs.log_level.upper()),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Preflight: ensure directories/files/logging are set up
    preflight_environment(config)

    seeds = config.seeds
    print(f"Loaded {len(seeds)} seed URLs from configuration")

    print("\n=== Crawler/Scraper Configuration ===")
    print(f"Run ID: {config.run_id}")
    print(f"Workspace: {config.workspace}")
    print(f"User Agent: {config.user_agent}")
    print(f"Rate Limit: {config.limits.req_per_sec} req/sec")
    print(f"Max Depth: {config.caps.max_depth}")
    print(f"HTML Store: {config.storage.html_store_root}")
    print(f"Metadata File: {config.storage.metadata_file}")
    print("====================================\n")

    asyncio.run(run_crawler(config, seeds))


if __name__ == "__main__":
    main()
