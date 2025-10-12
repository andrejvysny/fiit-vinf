"""CLI entry point for GitHub Crawler.

Provides commands for running the crawler and validating policies.
"""

import argparse
import asyncio
import sys
import os
from pathlib import Path
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from crawler.config import CrawlerConfig


def cmd_validate_policy(args):
    """Validate a URL against the crawler policy."""
    try:
        # Load config
        config = CrawlerConfig.from_yaml(args.config)
        
        # Import policy module (will be implemented in Step B)
        from crawler.policy import UrlPolicy
        from crawler.robots_cache import RobotsCache
        from crawler.url_tools import canonicalize
        
        # Initialize components
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        robots = RobotsCache(config.robots.user_agent, config.robots.cache_ttl_sec)
        policy = UrlPolicy(config, robots)
        
        # Validate URL
        url = args.url
        canonical = canonicalize(url)
        
        print(f"\nValidating URL: {url}")
        print(f"Canonical form: {canonical}")
        print("-" * 60)
        
        # Check policy
        result = loop.run_until_complete(policy.gate(url))
        
        print(f"Decision: {'✅ ALLOWED' if result['ok'] else '❌ DENIED'}")
        print(f"Reason: {result['reason']}")
        if result.get('page_type'):
            print(f"Page Type: {result['page_type']}")
        print(f"Canonical URL: {result['canonical_url']}")
        
        # Show robots verdict separately
        robots_allowed = loop.run_until_complete(robots.is_allowed(canonical))
        print(f"Robots.txt: {'✅ Allowed' if robots_allowed else '❌ Disallowed'}")
        
    except ImportError as e:
        print(f"Error: Policy modules not yet implemented. {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def cmd_status(args):
    """Show crawler status and metrics."""
    try:
        # Load config
        config = CrawlerConfig.from_yaml(args.config)
        workspace = config.get_workspace_path()
        
        print("\n" + "="*60)
        print("Crawler Status")
        print("="*60)
        print(f"Workspace: {workspace}")
        print(f"Run ID: {config.run_id}")
        
        # Check frontier size (will be implemented in Step C)
        frontier_path = Path(config.frontier.db_path)
        if frontier_path.exists():
            size_mb = frontier_path.stat().st_size / (1024 * 1024)
            print(f"Frontier DB: {size_mb:.2f} MB")
        else:
            print("Frontier DB: Not initialized")
        
        # Check discoveries spool
        discoveries_dir = Path(config.spool.discoveries_dir)
        if discoveries_dir.exists():
            files = list(discoveries_dir.glob("*.jsonl"))
            total_size = sum(f.stat().st_size for f in files) / (1024 * 1024)
            print(f"Discoveries: {len(files)} files, {total_size:.2f} MB")
        else:
            print("Discoveries: No output yet")
        
        # Check metrics
        metrics_path = Path(config.metrics.csv_path)
        if metrics_path.exists():
            with open(metrics_path, 'r') as f:
                lines = f.readlines()
            print(f"Metrics entries: {len(lines)}")
        else:
            print("Metrics: No data yet")
        
        # Check trajectory
        trajectory_path = Path(config.trajectory.csv_path)
        if trajectory_path.exists():
            with open(trajectory_path, 'r') as f:
                edges = len(f.readlines())
            print(f"Trajectory edges: {edges}")
        else:
            print("Trajectory: No edges yet")
        
        print("="*60)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def cmd_run(args):
    """Run the crawler."""
    try:
        # Load config
        config = CrawlerConfig.from_yaml(args.config)
        
        # Setup logging
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Load seeds
        seeds = []
        
        # Load from file if provided
        if args.seeds:
            seeds_path = Path(args.seeds)
            if seeds_path.exists():
                with open(seeds_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            seeds.append(line)
        
        # Add command-line seeds
        if args.seed:
            seeds.extend(args.seed)
        
        # Add default seeds if none provided
        if not seeds:
            seeds = [
                'https://github.com/topics',
                'https://github.com/trending'
            ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_seeds = []
        for s in seeds:
            if s not in seen:
                seen.add(s)
                unique_seeds.append(s)
        
        print(f"\nStarting crawler with {len(unique_seeds)} seeds")
        print("Seeds:")
        for s in unique_seeds[:10]:  # Show first 10
            print(f"  - {s}")
        if len(unique_seeds) > 10:
            print(f"  ... and {len(unique_seeds) - 10} more")
        
        # Import and run crawler
        from crawler.run import CrawlerService
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        service = CrawlerService(config)

        async def run_crawler():
            await service.start(unique_seeds)
            await service.run()
            await service.stop()
        
        try:
            loop.run_until_complete(run_crawler())
        except KeyboardInterrupt:
            print("\nShutting down crawler...")
            loop.run_until_complete(service.stop())
        finally:
            loop.close()
        
    except ImportError as e:
        print(f"Error: Crawler modules not yet implemented. {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GitHub Crawler - Discovery Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global arguments
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Start the crawler')
    run_parser.add_argument(
        '--seeds',
        help='Path to seeds file (one URL per line)'
    )
    run_parser.add_argument(
        '--seed',
        action='append',
        help='Add a seed URL (can be used multiple times)'
    )
    
    # Validate-policy command
    validate_parser = subparsers.add_parser(
        'validate-policy',
        help='Validate a URL against the crawler policy'
    )
    validate_parser.add_argument(
        '--url',
        required=True,
        help='URL to validate'
    )
    
    # Status command
    status_parser = subparsers.add_parser(
        'status',
        help='Show crawler status and metrics'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    # Dispatch to command handler
    if args.command == 'run':
        cmd_run(args)
    elif args.command == 'validate-policy':
        cmd_validate_policy(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
