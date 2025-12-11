#!/usr/bin/env python3
"""Spark HTML extraction job - extracts text and entities from crawled HTML.

Uses DataFrame API with mapPartitions for efficient parallel processing.
No pandas/numpy dependencies - pure Spark and native Python.

Architecture:
- DataFrame API for file paths distribution
- mapPartitions for partition-level processing
- Text files written directly in workers
- Entities written to partition files, merged on driver
- Only stats collected to driver (no large data transfer)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Add project root to path for imports
sys.path.insert(0, '/opt/app')

from config_loader import ConfigError, load_yaml_config
from spark.lib.extractor.config import ExtractorConfig
from spark.lib.extractor.io_utils import HtmlFileDiscovery
from spark.lib.extractor.outputs import text_exists, write_text
from spark.lib.extractor import html_clean, entity_extractors
from spark.lib.stats import PipelineStats, save_pipeline_summary


EntityRow = Tuple[str, str, str, str]
STAT_FIELDS: Sequence[str] = (
    "files_processed",
    "files_skipped",
    "text_written",
    "entities_extracted",
    "stars_found",
    "forks_found",
    "langs_found",
    "readme_found",
    "license_found",
    "topics_found",
    "urls_found",
    "emails_found",
)


@dataclass(frozen=True)
class JobOptions:
    input_root: Path
    text_out: Optional[Path]
    entities_out: Optional[Path]
    enable_text: bool
    enable_entities: bool
    force: bool
    dry_run: bool
    limit: Optional[int]
    partitions: int
    master: str
    app_name: str


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Spark HTML extraction job (DataFrame API)"
    )
    parser.add_argument(
        '--config',
        default='/opt/app/config.yml',
        help='Path to YAML configuration file'
    )
    parser.add_argument(
        '--input-root', '--in',
        dest='input_root',
        help='Override input directory for HTML files'
    )
    parser.add_argument(
        '--text-out',
        help='Override output directory for text files'
    )
    parser.add_argument(
        '--entities-out', '--out',
        dest='entities_out',
        help='Override output path for entities TSV'
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Limit number of files to process'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Alias for --sample'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing output files'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing'
    )
    parser.add_argument(
        '--no-text',
        action='store_true',
        help='Disable text extraction'
    )
    parser.add_argument(
        '--no-entities',
        action='store_true',
        help='Disable entity extraction'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=64,
        help='Number of Spark partitions'
    )
    parser.add_argument(
        '--master',
        default='local[*]',
        help='Spark master URL'
    )
    parser.add_argument(
        '--app-name',
        default='HTMLExtractor-DataFrame',
        help='Spark application name'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        help='Logging level'
    )
    return parser.parse_args(argv)


def _resolve_job_options(args: argparse.Namespace) -> JobOptions:
    """Resolve job options from CLI args and config file."""
    try:
        app_config = load_yaml_config(args.config)
    except ConfigError as exc:
        raise SystemExit(f"[spark extractor] Cannot load config: {exc}") from exc

    extractor_cfg = ExtractorConfig.from_app_config(app_config)

    input_root = Path(args.input_root or extractor_cfg.input_root).resolve()

    cli_limit = args.sample if args.sample is not None else args.limit
    cfg_limit = extractor_cfg.sample if extractor_cfg.sample is not None else extractor_cfg.limit
    limit = cli_limit if cli_limit is not None else cfg_limit

    force = bool(args.force or extractor_cfg.force)
    dry_run = bool(args.dry_run or extractor_cfg.dry_run)

    # Text output
    text_out_value = args.text_out or extractor_cfg.text_out
    enable_text_cfg = extractor_cfg.enable_text if extractor_cfg.enable_text is not None else True
    enable_text = enable_text_cfg and not args.no_text
    text_out = Path(text_out_value).resolve() if enable_text and text_out_value else None
    if text_out is None:
        enable_text = False

    # Entities output
    entities_value = args.entities_out or extractor_cfg.entities_out
    enable_entities_cfg = extractor_cfg.enable_entities if extractor_cfg.enable_entities is not None else True
    enable_entities = enable_entities_cfg and not args.no_entities
    entities_out = Path(entities_value).resolve() if enable_entities and entities_value else None
    if entities_out is None:
        enable_entities = False

    partitions = max(1, args.partitions)

    return JobOptions(
        input_root=input_root,
        text_out=text_out,
        entities_out=entities_out,
        enable_text=enable_text,
        enable_entities=enable_entities,
        force=force,
        dry_run=dry_run,
        limit=limit,
        partitions=partitions,
        master=args.master,
        app_name=args.app_name,
    )


def _configure_logging(level: str) -> logging.Logger:
    """Configure logging and return logger."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger("spark_html_extractor")


def _build_spark_session(opts: JobOptions) -> SparkSession:
    """Build Spark session with optimized configuration."""
    builder = (
        SparkSession.builder.appName(opts.app_name)
        .master(opts.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '6g'))
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '4g'))
        .config("spark.driver.maxResultSize", os.environ.get('SPARK_MAX_RESULT_SIZE', '2g'))
        .config("spark.sql.shuffle.partitions", str(opts.partitions))
        .config("spark.default.parallelism", str(opts.partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
    )
    return builder.getOrCreate()


def _sanitize_field(value: str) -> str:
    """Sanitize field value for TSV output."""
    return value.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def process_partition(
    iterator: Iterator[Row],
    input_root: str,
    text_out: Optional[str],
    entities_parts_dir: str,
    enable_text: bool,
    enable_entities: bool,
    force: bool,
    dry_run: bool
) -> Iterator[Row]:
    """
    Process HTML files in partition - runs via mapPartitions.

    Single-pass architecture:
    1. Read HTML files
    2. Extract text and write directly to text_out
    3. Extract entities and write to partition-specific TSV
    4. Return only stats as Row (no large data to driver)
    """
    import threading
    import uuid

    # Re-import in worker context
    sys.path.insert(0, '/opt/app')
    from spark.lib.extractor import html_clean, entity_extractors
    from spark.lib.extractor.outputs import text_exists, write_text

    log = logging.getLogger("spark_html_extractor.worker")

    input_root_path = Path(input_root)
    text_out_path = Path(text_out) if text_out else None
    entities_parts_path = Path(entities_parts_dir)
    entities_parts_path.mkdir(parents=True, exist_ok=True)

    # Create partition-specific entity file with unique ID
    partition_id = f"{os.getpid()}_{threading.get_ident()}_{uuid.uuid4().hex[:8]}"
    entity_file = entities_parts_path / f"part-{partition_id}.tsv"

    # Initialize stats
    stats = {field: 0 for field in STAT_FIELDS}

    with open(entity_file, 'w', encoding='utf-8') as ef:
        for input_row in iterator:
            path_str = input_row['file_path']
            path = Path(path_str)
            doc_id = path.stem

            # Skip if text already exists and not forcing
            if (
                not force
                and not enable_entities
                and enable_text
                and text_out_path is not None
                and text_exists(text_out_path, path, doc_id, input_root_path)
            ):
                stats["files_skipped"] += 1
                continue

            # Read HTML content
            try:
                html_content = path.read_text(encoding="utf-8", errors="ignore")
            except Exception as exc:
                log.warning("Failed to read %s: %s", path, exc)
                stats["files_processed"] += 1
                continue

            if not html_content.strip():
                log.debug("Empty HTML file: %s", path)
                stats["files_processed"] += 1
                continue

            # Write text in worker
            if enable_text and text_out_path is not None and not dry_run:
                try:
                    raw_text = html_clean.html_to_text(html_content, strip_boilerplate=False)
                    if raw_text:
                        if write_text(text_out_path, path, doc_id, raw_text, input_root_path, force=force):
                            stats["text_written"] += 1
                except Exception as exc:
                    log.warning("Failed to write text for %s: %s", path, exc)

            # Extract entities and write to partition file
            if enable_entities and not dry_run:
                try:
                    entities = entity_extractors.extract_all_entities(doc_id, html_content)
                    entity_types = {e[1] for e in entities}

                    if "STAR_COUNT" in entity_types:
                        stats["stars_found"] += 1
                    if "FORK_COUNT" in entity_types:
                        stats["forks_found"] += 1
                    if "LANG_STATS" in entity_types:
                        stats["langs_found"] += 1
                    if "README" in entity_types:
                        stats["readme_found"] += 1
                    if "LICENSE" in entity_types:
                        stats["license_found"] += 1
                    if "TOPICS" in entity_types:
                        stats["topics_found"] += 1
                    if "URL" in entity_types:
                        stats["urls_found"] += 1
                    if "EMAIL" in entity_types:
                        stats["emails_found"] += 1

                    stats["entities_extracted"] += len(entities)

                    # Write entities to partition file
                    for entity in entities:
                        doc_id_e, type_e, value_e, offsets_e = entity[0], entity[1], entity[2], entity[3]
                        value_e = _sanitize_field(value_e)
                        offsets_e = _sanitize_field(offsets_e)
                        ef.write(f"{doc_id_e}\t{type_e}\t{value_e}\t{offsets_e}\n")

                except Exception as exc:
                    log.warning("Failed to extract entities for %s: %s", path, exc)

            stats["files_processed"] += 1

    # Yield stats as Row
    yield Row(**stats)


def run_extraction(
    spark: SparkSession,
    html_files: Sequence[Path],
    opts: JobOptions,
    logger: logging.Logger
) -> dict:
    """
    Run the Spark extraction job using DataFrame API with mapPartitions.

    Returns dict with extraction statistics.
    """
    # Create DataFrame from file paths
    path_strings = [str(p) for p in html_files]
    partitions = min(max(1, opts.partitions), len(path_strings)) if path_strings else 1
    logger.info("Processing %d HTML files with %d partitions", len(path_strings), partitions)

    # Create file paths DataFrame
    schema = StructType([StructField("file_path", StringType(), False)])
    files_df = spark.createDataFrame([(p,) for p in path_strings], schema=schema)
    files_df = files_df.repartition(partitions)

    # Setup entities partition directory
    entities_parts_dir = None
    if opts.enable_entities and opts.entities_out:
        entities_parts_dir = opts.entities_out.parent / '_entity_parts'
        if entities_parts_dir.exists():
            shutil.rmtree(entities_parts_dir)
        entities_parts_dir.mkdir(parents=True, exist_ok=True)

    # Define output schema for stats
    stats_schema = StructType([
        StructField(field, LongType(), False) for field in STAT_FIELDS
    ])

    # Create wrapper function that captures configuration
    input_root_str = str(opts.input_root)
    text_out_str = str(opts.text_out) if opts.text_out else None
    entities_parts_str = str(entities_parts_dir) if entities_parts_dir else "/tmp/entities_parts"
    enable_text = opts.enable_text
    enable_entities = opts.enable_entities
    force = opts.force
    dry_run = opts.dry_run

    def partition_processor(iterator: Iterator[Row]) -> Iterator[Row]:
        return process_partition(
            iterator,
            input_root_str,
            text_out_str,
            entities_parts_str,
            enable_text,
            enable_entities,
            force,
            dry_run
        )

    # Process using mapPartitions on RDD, then convert to DataFrame
    stats_rdd = files_df.rdd.mapPartitions(partition_processor)
    stats_df = spark.createDataFrame(stats_rdd, schema=stats_schema)

    # Aggregate stats (collect only small data)
    agg_exprs = [F.sum(field).alias(field) for field in STAT_FIELDS]
    aggregated = stats_df.agg(*agg_exprs).collect()[0]
    stats = {field: int(aggregated[field] or 0) for field in STAT_FIELDS}

    # Merge entity partition files into final TSV
    if opts.enable_entities and opts.entities_out and entities_parts_dir:
        part_files = sorted(entities_parts_dir.glob("part-*.tsv"))
        with open(opts.entities_out, 'w', encoding='utf-8') as out:
            out.write("doc_id\ttype\tvalue\toffsets_json\n")
            for part_file in part_files:
                with open(part_file, 'r', encoding='utf-8') as pf:
                    for line in pf:
                        out.write(line)
        logger.info("Wrote entities TSV to %s", opts.entities_out)

        # Clean up partition files
        shutil.rmtree(entities_parts_dir, ignore_errors=True)

    return stats


def generate_manifest(
    opts: JobOptions,
    stats: dict,
    duration: float,
    file_count: int
) -> dict:
    """Generate run manifest JSON."""
    code_files = list(Path('/opt/app/spark').glob('**/*.py'))
    code_hash = hashlib.sha256()
    for file in sorted(code_files):
        if file.is_file():
            code_hash.update(file.read_bytes())

    return {
        'timestamp': datetime.now().isoformat(),
        'duration_seconds': round(duration, 2),
        'code_hash': code_hash.hexdigest()[:16],
        'api': 'DataFrame',
        'arguments': {
            'input_root': str(opts.input_root),
            'text_out': str(opts.text_out) if opts.text_out else None,
            'entities_out': str(opts.entities_out) if opts.entities_out else None,
            'limit': opts.limit,
            'partitions': opts.partitions,
            'force': opts.force,
            'dry_run': opts.dry_run,
        },
        'inputs': {
            'total_files': file_count,
            'processed_files': file_count if not opts.dry_run else 0,
        },
        'outputs': stats,
        'spark_config': {
            'driver_memory': os.environ.get('SPARK_DRIVER_MEMORY', 'default'),
            'executor_memory': os.environ.get('SPARK_EXECUTOR_MEMORY', 'default'),
            'partitions': opts.partitions,
        },
    }


def save_pipeline_stats(opts: JobOptions, stats: dict, duration: float, file_count: int, logger: logging.Logger) -> None:
    """Save pipeline statistics."""
    try:
        pipeline_stats = PipelineStats("html_extraction")
        pipeline_stats.set_config(
            mode="spark_dataframe",
            partitions=opts.partitions,
            force=opts.force,
            sample=opts.limit
        )
        pipeline_stats.set_inputs(
            input_dir=str(opts.input_root),
            total_files=file_count,
            total_items=file_count
        )
        pipeline_stats.set_outputs(
            text_files_written=stats.get('text_written', 0),
            entities_written=stats.get('entities_extracted', 0),
            errors=0,
            entities_dir=str(opts.entities_out.parent) if opts.entities_out else '',
            text_dir=str(opts.text_out) if opts.text_out else ''
        )

        # Entity type breakdown
        for entity_type, stat_key in [
            ('STAR_COUNT', 'stars_found'),
            ('FORK_COUNT', 'forks_found'),
            ('LANG_STATS', 'langs_found'),
            ('README', 'readme_found'),
            ('LICENSE', 'license_found'),
            ('TOPICS', 'topics_found'),
            ('URL', 'urls_found'),
            ('EMAIL', 'emails_found'),
        ]:
            if stats.get(stat_key, 0) > 0:
                pipeline_stats.set_entities(entity_type, stats[stat_key])

        # Performance metrics
        if duration > 0:
            pipeline_stats.set_nested("performance", "files_per_second", round(file_count / duration, 2))
            pipeline_stats.set_nested("performance", "entities_per_second",
                                      round(stats.get('entities_extracted', 0) / duration, 2))

        pipeline_stats.finalize("completed")
        stats_path = pipeline_stats.save()
        logger.info("Stats saved to: %s", stats_path)

        # Generate pipeline summary
        save_pipeline_summary()
    except Exception as e:
        logger.warning("Failed to save pipeline stats: %s", e)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main entry point."""
    start_time = time.time()

    args = parse_args(argv)
    logger = _configure_logging(args.log_level)

    try:
        opts = _resolve_job_options(args)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Failed to resolve options: %s", exc)
        return 1

    # Discover HTML files
    logger.info("Discovering HTML files in %s", opts.input_root)
    html_files = HtmlFileDiscovery(opts.input_root).discover(limit=opts.limit)

    if not html_files:
        logger.warning("No HTML files found under %s", opts.input_root)
        return 0

    logger.info("Found %d HTML files to process", len(html_files))

    if opts.dry_run:
        logger.info("Dry-run enabled; listing files only.")
        for i, path in enumerate(html_files[:10], 1):
            print(f"  {i}. {path}")
        if len(html_files) > 10:
            print(f"  ... and {len(html_files) - 10} more")
        return 0

    # Build Spark session
    spark = _build_spark_session(opts)
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Run extraction
        stats = run_extraction(spark, html_files, opts, logger)
        duration = time.time() - start_time

        # Generate and save manifest
        manifest = generate_manifest(opts, stats, duration, len(html_files))

        runs_dir = Path('/opt/app/runs')
        runs_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_dir = runs_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        manifest_file = run_dir / 'manifest.json'
        manifest_file.write_text(json.dumps(manifest, indent=2))

        # Save pipeline stats
        save_pipeline_stats(opts, stats, duration, len(html_files), logger)

        # Print summary
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE (DataFrame API)")
        logger.info("Duration: %.2f seconds", duration)
        logger.info("Files processed: %d", stats.get('files_processed', 0))
        logger.info("Files skipped: %d", stats.get('files_skipped', 0))
        logger.info("Text files written: %d", stats.get('text_written', 0))
        logger.info("Entities extracted: %d", stats.get('entities_extracted', 0))
        logger.info("Manifest saved to: %s", manifest_file)
        logger.info("=" * 60)

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
