import argparse
import asyncio
import logging
import sys
from pathlib import Path

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
