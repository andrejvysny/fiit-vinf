import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

from crawler.config import CrawlerScraperConfig
from crawler.service import CrawlerScraperService



def load_seeds(seeds_file: str) -> list:
    seeds = []
    if not Path(seeds_file).exists():
        print(f"Error: Seeds file not found: {seeds_file}")
        sys.exit(1)
    with open(seeds_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                seeds.append(line)
    return seeds


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _ensure_file(p: Path, content: Optional[str] = "") -> bool:
    """Ensure file exists. Returns True if file was created."""
    if not p.exists():
        if p.parent and not p.parent.exists():
            _ensure_dir(p.parent)
        with open(p, "w", encoding="utf-8") as f:
            if content is not None:
                f.write(content)
        return True
    return False


def _attach_file_logging(log_path: Path, level: str) -> None:
    """Attach a file handler to root logger if not already present."""
    logger = logging.getLogger()
    # Avoid duplicate handlers for the same file
    for h in logger.handlers:
        if isinstance(h, logging.FileHandler):
            try:
                if Path(getattr(h, 'baseFilename', '')) == log_path:
                    return
            except Exception:
                continue

    _ensure_dir(log_path.parent)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def preflight_environment(config: CrawlerScraperConfig, seeds_file: str) -> None:
    """Validate and prepare workspace, storage paths, log file, and seeds file."""
    ws = Path(config.workspace).resolve()

    # Ensure workspace root
    _ensure_dir(ws)

    # Ensure storage directories
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

    # Ensure initial files exist (do not overwrite if present)
    created_frontier = _ensure_file(frontier_path, "")
    created_fetched = _ensure_file(fetched_urls_path, "")
    created_robots = _ensure_file(robots_cache_path, "")
    created_metadata = _ensure_file(metadata_path, "")

    # Attach file logging now that directory exists
    _attach_file_logging(logs_path, config.logs.log_level)

    # Ensure seeds file exists
    seeds_path = Path(seeds_file).resolve()
    default_seeds = (
        "# Default GitHub seeds (edit as needed)\n"
        "https://github.com/python/cpython\n"
        "https://github.com/torvalds/linux\n"
        "https://github.com/tensorflow/tensorflow\n"
        "https://github.com/kubernetes/kubernetes\n"
        "https://github.com/facebook/react\n"
        "https://github.com/vuejs/vue\n"
        "https://github.com/golang/go\n"
        "https://github.com/rust-lang/rust\n"
        "https://github.com/microsoft/vscode\n"
        "https://github.com/nodejs/node\n"
    )
    created_seeds = _ensure_file(seeds_path, default_seeds)

    # Log a concise summary of the preflight actions
    logging.getLogger(__name__).info(
        "Preflight: workspace prepared at %s; created files -> frontier:%s fetched:%s robots:%s metadata:%s seeds:%s; logs -> %s",
        str(ws),
        created_frontier,
        created_fetched,
        created_robots,
        created_metadata,
        created_seeds,
        str(logs_path),
    )


async def run_crawler(config: CrawlerScraperConfig, seeds: list):
    service = CrawlerScraperService(config)
    
    try:
        await service.start(seeds)
        await service.run()
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await service.stop()


def main():
    parser = argparse.ArgumentParser(
        description='GitHub Crawler/Scraper'
    )
    
    parser.add_argument(
        '--config',
        required=False,
        help='Path to unified config YAML file',
        default='config.yaml'
    )
    
    parser.add_argument(
        '--seeds',
        required=False,
        help='Path to seeds file (one URL per line)',
        default='seeds.txt'
    )
    
    args = parser.parse_args()
    
    print(f"Loading configuration from: {args.config}")
    config = CrawlerScraperConfig.from_yaml(args.config)

    logging.basicConfig(
        level=getattr(logging, config.logs.log_level.upper()),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Preflight: ensure directories/files/logging are set up
    preflight_environment(config, args.seeds)
    
    print(f"Loading seeds from: {args.seeds}")
    seeds = load_seeds(args.seeds)
    print(f"Loaded {len(seeds)} seed URLs")
    
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
if __name__ == '__main__':
    main()
