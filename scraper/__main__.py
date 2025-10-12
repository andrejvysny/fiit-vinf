"""
Scraper CLI entry point - implements vi-scrape command
"""

import sys
import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Optional

from .config import ScraperConfig
from .util.signals import SignalHandler
from .control.flags import ControlFlags


def cmd_status(args):
    """Show scraper status"""
    try:
        cfg = ScraperConfig.from_yaml(args.config)

        print("\nSCRAPER STATUS")
        print("=" * 60)
        print(f"Run ID: {cfg.run_id}")
        print(f"Workspace: {cfg.workspace}")
        print()

        # Check bookmark
        bookmark_path = Path(cfg.bookmark.path)
        if bookmark_path.exists():
            bookmark = json.loads(bookmark_path.read_text())
            print(f"Bookmark:")
            print(f"  File: {bookmark.get('file', 'None')}")
            print(f"  Offset: {bookmark.get('offset', 0)} bytes")
        else:
            print("Bookmark: Not found (fresh start)")
        print()

        # Check spool size
        spool_dir = Path(cfg.spool.dir)
        if spool_dir.exists():
            total_size = 0
            file_count = 0
            for f in spool_dir.glob("discoveries-*.jsonl"):
                file_count += 1
                total_size += f.stat().st_size
            print(f"Spool:")
            print(f"  Directory: {cfg.spool.dir}")
            print(f"  Files: {file_count}")
            print(f"  Total size: {total_size / (1024**3):.3f} GB")
            print(f"  Backpressure threshold: {cfg.spool.max_backlog_gb} GB")
        else:
            print(f"Spool: Directory not found ({cfg.spool.dir})")
        print()

        # Check stored HTML
        store_dir = Path(cfg.store.root)
        if store_dir.exists():
            html_count = len(list(store_dir.rglob("*.html*")))
            print(f"Stored HTML: {html_count} files")
        else:
            print("Stored HTML: Directory not found")
        print()

        # Check pages CSV
        pages_csv = Path(cfg.pages_csv.path)
        if pages_csv.exists():
            with open(pages_csv, 'r') as f:
                line_count = sum(1 for _ in f) - 1  # Minus header
            print(f"Pages CSV: {line_count} rows")
        else:
            print("Pages CSV: Not found")
        print()

        # Check control flags
        flags = ControlFlags(cfg.workspace)
        print(f"Control Flags:")
        print(f"  Paused: {'YES' if flags.is_paused() else 'NO'}")
        print(f"  Stop requested: {'YES' if flags.should_stop() else 'NO'}")

        print("=" * 60)
        print()

    except Exception as e:
        print(f"ERROR: Failed to get status: {e}", file=sys.stderr)
        return 1

    return 0


def cmd_run(args):
    """Start scraper (fresh or resume if bookmark exists)"""
    try:
        cfg = ScraperConfig.from_yaml(args.config)

        # Check if this is a resume
        bookmark_path = Path(cfg.bookmark.path)
        if bookmark_path.exists():
            print(f"Found existing bookmark - resuming from last position")

        print(f"\nStarting scraper with run_id: {cfg.run_id}")
        print(f"Workspace: {cfg.workspace}")
        print(f"Mode: RUN (fresh or auto-resume)\n")

        # Import here to avoid circular dependencies
        from .run import ScraperService

        # Setup signal handlers
        signal_handler = SignalHandler()
        signal_handler.setup()

        # Create and run service
        service = ScraperService(cfg, signal_handler=signal_handler)

        # Run async event loop
        try:
            asyncio.run(service.run())
        except KeyboardInterrupt:
            print("\nShutdown complete")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 0


def cmd_resume(args):
    """Resume scraper strictly from bookmark"""
    try:
        cfg = ScraperConfig.from_yaml(args.config)

        # Check bookmark exists
        bookmark_path = Path(cfg.bookmark.path)
        if not bookmark_path.exists():
            print("ERROR: No bookmark found - cannot resume", file=sys.stderr)
            print(f"Bookmark path: {cfg.bookmark.path}", file=sys.stderr)
            return 1

        bookmark = json.loads(bookmark_path.read_text())
        print(f"Resuming from bookmark:")
        print(f"  File: {bookmark.get('file')}")
        print(f"  Offset: {bookmark.get('offset')} bytes")
        print()

        # Import here to avoid circular dependencies
        from .run import ScraperService

        # Setup signal handlers
        signal_handler = SignalHandler()
        signal_handler.setup()

        # Create and run service
        service = ScraperService(cfg, signal_handler=signal_handler)

        # Run async event loop
        try:
            asyncio.run(service.run())
        except KeyboardInterrupt:
            print("\nShutdown complete")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 0


def cmd_replay(args):
    """Replay - rebuild CSV from stored HTML (no network)"""
    try:
        cfg = ScraperConfig.from_yaml(args.config)

        print(f"REPLAY MODE - Rebuilding CSV from stored HTML")
        print(f"Store: {cfg.store.root}")
        print(f"Output: {cfg.pages_csv.path}")
        print()

        # Import here to avoid circular dependencies
        from .run import ScraperService

        service = ScraperService(cfg)

        # Run replay
        asyncio.run(service.replay())

        print("\nReplay complete")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 0


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        prog="vi-scrape",
        description="VI2025 Scraper - GitHub HTML content scraper"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Start scraper (fresh or auto-resume)")
    run_parser.add_argument("--config", required=True, help="Path to configuration YAML")

    # Resume command
    resume_parser = subparsers.add_parser("resume", help="Resume strictly from bookmark")
    resume_parser.add_argument("--config", required=True, help="Path to configuration YAML")

    # Replay command
    replay_parser = subparsers.add_parser("replay", help="Rebuild CSV from stored HTML (no network)")
    replay_parser.add_argument("--config", required=True, help="Path to configuration YAML")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show scraper status")
    status_parser.add_argument("--config", required=True, help="Path to configuration YAML")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Route to command handler
    if args.command == "run":
        return cmd_run(args)
    elif args.command == "resume":
        return cmd_resume(args)
    elif args.command == "replay":
        return cmd_replay(args)
    elif args.command == "status":
        return cmd_status(args)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())