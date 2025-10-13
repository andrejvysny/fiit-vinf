"""Unified Crawler CLI - main entry point.

No separate scraper needed - single crawler fetches, stores, and extracts.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from crawler.config_unified import UnifiedCrawlerConfig
from crawler.run_unified import UnifiedCrawlerService


def setup_logging(log_level: str):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def load_seeds(seeds_file: str) -> list:
    """Load seed URLs from file."""
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


async def run_crawler(config: UnifiedCrawlerConfig, seeds: list):
    """Run unified crawler."""
    service = UnifiedCrawlerService(config)
    
    try:
        # Initialize and seed
        await service.start(seeds)
        
        # Run main loop
        await service.run()
        
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        await service.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Unified GitHub Crawler (fetch + store in one operation)'
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
    
    # Load config
    print(f"Loading configuration from: {args.config}")
    config = UnifiedCrawlerConfig.from_yaml(args.config)
    
    # Setup logging
    setup_logging(config.logs.log_level)
    
    # Load seeds
    print(f"Loading seeds from: {args.seeds}")
    seeds = load_seeds(args.seeds)
    print(f"Loaded {len(seeds)} seed URLs")
    
    # Print config summary
    print("\n=== Unified Crawler Configuration ===")
    print(f"Run ID: {config.run_id}")
    print(f"Workspace: {config.workspace}")
    print(f"User Agent: {config.user_agent}")
    print(f"Rate Limit: {config.limits.req_per_sec} req/sec")
    print(f"Max Depth: {config.caps.max_depth}")
    print(f"HTML Store: {config.storage.html_store_root}")
    print(f"Metadata File: {config.storage.metadata_file}")
    print("====================================\n")
    
    # Run
    asyncio.run(run_crawler(config, seeds))


if __name__ == '__main__':
    main()
