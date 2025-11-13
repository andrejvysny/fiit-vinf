#!/usr/bin/env python3
"""
PySpark HTML extractor - processes HTML files to extract text and entities
Optimized for single-host Docker execution with filesystem-based I/O
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession
from pyspark import SparkContext

# Add project root to Python path for imports
sys.path.insert(0, '/opt/app')

from config_loader import load_yaml_config
from extractor.config import ExtractorConfig
from extractor.html_clean import html_to_text
from extractor.entity_extractors import extract_all_entities


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="PySpark HTML to text/entity extractor"
    )
    parser.add_argument(
        '--in',
        dest='input_dir',
        default='/opt/app/workspace/store/html',
        help='Input directory with HTML files'
    )
    parser.add_argument(
        '--out',
        dest='output_dir',
        default='/opt/app/workspace/store/spark',
        help='Output directory for text and entities'
    )
    parser.add_argument(
        '--config',
        default='/opt/app/config.yml',
        help='Path to config.yml'
    )
    parser.add_argument(
        '--sample',
        type=int,
        help='Process only first N files (for testing)'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=64,
        help='Number of Spark partitions (default: 64)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing outputs'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing'
    )
    return parser.parse_args(argv)


def discover_html_files(input_dir: Path, limit: Optional[int] = None) -> List[Path]:
    """Discover HTML files recursively in input directory"""
    html_files = []

    # Walk through all subdirectories
    for root, dirs, files in os.walk(input_dir):
        # Sort for deterministic ordering
        dirs.sort()
        files.sort()

        for file in files:
            if file.endswith('.html'):
                html_files.append(Path(root) / file)

                if limit and len(html_files) >= limit:
                    return html_files[:limit]

    return html_files


def process_html_batch(
    partition_idx: int,
    file_paths: Iterator[str]
) -> Iterator[Tuple[str, str, List[Tuple[str, str, str, str]]]]:
    """
    Process a batch of HTML files in a partition
    Returns: (doc_id, text_content, entities_list)
    """
    import html.parser
    import re

    # Configure logging for worker
    logging.basicConfig(
        level=logging.INFO,
        format=f'[Partition {partition_idx}] %(asctime)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    processed = 0
    for file_path_str in file_paths:
        try:
            file_path = Path(file_path_str)
            doc_id = file_path.stem

            # Read HTML content
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                html_content = f.read()

            if not html_content.strip():
                logger.warning(f"Empty file: {file_path}")
                continue

            # Extract text (using the project's html_clean module)
            try:
                text = html_to_text(html_content, strip_boilerplate=False)
            except Exception as e:
                logger.error(f"Failed to extract text from {file_path}: {e}")
                text = ""

            # Extract entities (using the project's entity_extractors)
            try:
                entities = extract_all_entities(doc_id, html_content)
            except Exception as e:
                logger.error(f"Failed to extract entities from {file_path}: {e}")
                entities = []

            processed += 1
            if processed % 100 == 0:
                logger.info(f"Processed {processed} files")

            yield (doc_id, text, entities)

        except Exception as e:
            logger.error(f"Error processing {file_path_str}: {e}")
            continue


def write_outputs(
    spark: SparkSession,
    results_rdd,
    output_dir: Path,
    force: bool = False
) -> dict:
    """Write text files and entities TSV"""
    stats = {
        'text_files_written': 0,
        'entities_written': 0,
        'errors': 0
    }

    # Ensure output directories exist
    text_dir = output_dir / 'text'
    entities_dir = output_dir / 'entities'
    text_dir.mkdir(parents=True, exist_ok=True)
    entities_dir.mkdir(parents=True, exist_ok=True)

    # Collect results (for sample runs this is manageable)
    results = results_rdd.collect()

    # Write text files
    for doc_id, text, _ in results:
        if text:
            text_file = text_dir / f"{doc_id}.txt"
            if force or not text_file.exists():
                try:
                    text_file.write_text(text, encoding='utf-8')
                    stats['text_files_written'] += 1
                except Exception as e:
                    logging.error(f"Failed to write text for {doc_id}: {e}")
                    stats['errors'] += 1

    # Collect and write entities TSV
    entities_file = entities_dir / 'entities.tsv'

    # Write header if new file or force
    if force or not entities_file.exists():
        with open(entities_file, 'w', encoding='utf-8') as f:
            f.write("doc_id\ttype\tvalue\toffsets_json\n")

    # Append entities
    with open(entities_file, 'a', encoding='utf-8') as f:
        for doc_id, _, entities in results:
            for entity in entities:
                # Extract fields based on the entity tuple structure
                if len(entity) >= 4:
                    _, entity_type, value, offsets_json = entity[:4]
                else:
                    continue

                # Sanitize TSV fields
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                offsets_json = offsets_json.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')

                f.write(f"{doc_id}\t{entity_type}\t{value}\t{offsets_json}\n")
                stats['entities_written'] += 1

    return stats


def generate_manifest(
    args: argparse.Namespace,
    stats: dict,
    duration: float,
    file_count: int
) -> dict:
    """Generate run manifest JSON"""

    # Calculate code hash
    code_files = list(Path('/opt/app/spark').glob('**/*.py'))
    code_hash = hashlib.sha256()
    for file in sorted(code_files):
        if file.is_file():
            code_hash.update(file.read_bytes())

    manifest = {
        'timestamp': datetime.now().isoformat(),
        'duration_seconds': round(duration, 2),
        'code_hash': code_hash.hexdigest()[:16],
        'arguments': {
            'input_dir': str(args.input_dir),
            'output_dir': str(args.output_dir),
            'sample': args.sample,
            'partitions': args.partitions,
            'force': args.force,
            'dry_run': args.dry_run
        },
        'inputs': {
            'total_files': file_count,
            'processed_files': file_count if not args.dry_run else 0
        },
        'outputs': stats,
        'spark_config': {
            'driver_memory': os.environ.get('SPARK_DRIVER_MEMORY', 'default'),
            'executor_memory': os.environ.get('SPARK_EXECUTOR_MEMORY', 'default'),
            'partitions': args.partitions
        }
    }

    return manifest


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main entry point"""
    start_time = time.time()

    # Parse arguments
    args = parse_args(argv)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Paths
    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        return 1

    # Discover HTML files
    logger.info(f"Discovering HTML files in {input_dir}")
    html_files = discover_html_files(input_dir, limit=args.sample)

    if not html_files:
        logger.warning("No HTML files found")
        return 0

    logger.info(f"Found {len(html_files)} HTML files to process")

    # Dry run - just list files
    if args.dry_run:
        logger.info("Dry run mode - listing files only:")
        for i, file in enumerate(html_files[:10], 1):
            print(f"  {i}. {file}")
        if len(html_files) > 10:
            print(f"  ... and {len(html_files) - 10} more")
        return 0

    # Create Spark session
    spark = SparkSession.builder \
        .appName("HTML-Extractor") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '4g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '2g')) \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .config("spark.default.parallelism", str(args.partitions)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Create RDD of file paths
        file_paths = [str(f) for f in html_files]
        partitions = min(args.partitions, len(file_paths))
        logger.info(f"Processing with {partitions} partitions")

        files_rdd = spark.sparkContext.parallelize(file_paths, partitions)

        # Process files using mapPartitionsWithIndex
        results_rdd = files_rdd.mapPartitionsWithIndex(process_html_batch)

        # Cache results for multiple operations
        results_rdd = results_rdd.cache()

        # Write outputs
        logger.info(f"Writing outputs to {output_dir}")
        stats = write_outputs(spark, results_rdd, output_dir, args.force)

        # Calculate duration
        duration = time.time() - start_time

        # Generate and save manifest
        manifest = generate_manifest(args, stats, duration, len(html_files))

        # Save manifest
        runs_dir = Path('/opt/app/runs')
        runs_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_dir = runs_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        manifest_file = run_dir / 'manifest.json'
        manifest_file.write_text(json.dumps(manifest, indent=2))

        # Print summary
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Files processed: {len(html_files)}")
        logger.info(f"Text files written: {stats['text_files_written']}")
        logger.info(f"Entities extracted: {stats['entities_written']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info(f"Manifest saved to: {manifest_file}")
        logger.info("=" * 60)

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())