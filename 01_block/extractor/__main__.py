"""Unified CLI entry point for the extractor package.

The extractor pipeline can be executed with:

    python -m extractor
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Sequence

from config_loader import ConfigError, load_yaml_config
from extractor.config import ExtractorConfig
from extractor.pipeline import ExtractorPipeline


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for the extractor CLI."""
    parser = argparse.ArgumentParser(
        description="Extract repository metadata and raw text from HTML files."
    )

    parser.add_argument(
        '--config',
        default='config.yml',
        help='Path to unified configuration file (default: %(default)s).',
    )
    parser.add_argument(
        '--in',
        '--input',
        dest='input_root',
        default=None,
        help='Input directory containing HTML files (defaults to extractor.input_root).',
    )
    parser.add_argument(
        '--text-out',
        dest='text_out',
        default=None,
        help='Output directory for plain text (defaults to extractor.outputs.text).',
    )
    parser.add_argument(
        '--entities-out',
        dest='entities_out',
        default=None,
        help='Output TSV file for entities (defaults to extractor.outputs.entities).',
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Maximum number of files to process',
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Process only N sample files for testing (alias for --limit)',
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force reprocessing of all files (overwrite existing outputs)',
    )
    parser.add_argument(
        '--no-text',
        action='store_true',
        help='Skip raw text extraction (disable workspace/store/text/)',
    )
    parser.add_argument(
        '--no-entities',
        action='store_true',
        help='Skip entity extraction (disable workspace/store/entities/)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing',
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable debug logging',
    )

    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Plugin-style entry point for running the extractor CLI."""
    args = parse_args(argv)

    try:
        app_config = load_yaml_config(args.config)
    except ConfigError as exc:
        print(f"[extractor] Failed to load configuration: {exc}", file=sys.stderr)
        return 1

    extractor_cfg = ExtractorConfig.from_app_config(app_config)

    verbose = bool(args.verbose or extractor_cfg.verbose)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    )

    input_root = Path(args.input_root or extractor_cfg.input_root).resolve()

    cli_limit = args.sample if args.sample is not None else args.limit
    cfg_limit = extractor_cfg.sample if extractor_cfg.sample is not None else extractor_cfg.limit
    limit = cli_limit if cli_limit is not None else cfg_limit

    force = bool(args.force or extractor_cfg.force)
    dry_run = bool(args.dry_run or extractor_cfg.dry_run)

    text_out_value = args.text_out or extractor_cfg.text_out
    text_requested = args.text_out is not None
    enable_text = (extractor_cfg.enable_text or text_requested) and not args.no_text
    text_out = Path(text_out_value).resolve() if enable_text and text_out_value else None

    entities_value = args.entities_out or extractor_cfg.entities_out
    entities_requested = args.entities_out is not None
    enable_entities = (extractor_cfg.enable_entities or entities_requested) and not args.no_entities
    entities_out = Path(entities_value).resolve() if enable_entities and entities_value else None

    pipeline = ExtractorPipeline(
        input_root,
        limit=limit,
        force=force,
        dry_run=dry_run,
        text_out=text_out,
        entities_out=entities_out,
    )

    return pipeline.run()


if __name__ == "__main__":
    raise SystemExit(main())
