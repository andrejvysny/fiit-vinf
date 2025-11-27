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


def write_text_partition(
    partition_idx: int,
    records: Iterator[Tuple[str, str, List[Tuple[str, str, str, str]]]],
    text_dir: str,
    force: bool
) -> Iterator[Tuple[int, int, int]]:
    """
    Write text files for a partition - runs on workers, not driver.
    Yields (text_written, entities_count, errors) per record.
    """
    text_path = Path(text_dir)
    text_path.mkdir(parents=True, exist_ok=True)

    for doc_id, text, entities in records:
        text_written = 0
        errors = 0

        if text:
            text_file = text_path / f"{doc_id}.txt"
            if force or not text_file.exists():
                try:
                    text_file.write_text(text, encoding='utf-8')
                    text_written = 1
                except Exception as e:
                    logging.error(f"[Partition {partition_idx}] Failed to write text for {doc_id}: {e}")
                    errors = 1

        yield (text_written, len(entities), errors)


def write_outputs_distributed(
    spark: SparkSession,
    results_rdd,
    output_dir: Path,
    force: bool = False
) -> dict:
    """
    Write text files and entities TSV using distributed processing.

    Text files are written by workers using foreachPartition.
    Entities are written using Spark DataFrame for scalability.
    Stats are aggregated using reduce (only small tuples collected).
    """
    from pyspark.sql.types import StructType, StructField, StringType

    stats = {
        'text_files_written': 0,
        'entities_written': 0,
        'errors': 0
    }

    # Ensure output directories exist (on driver - they're shared via mount)
    text_dir = output_dir / 'text'
    entities_dir = output_dir / 'entities'
    text_dir.mkdir(parents=True, exist_ok=True)
    entities_dir.mkdir(parents=True, exist_ok=True)

    # Broadcast configuration to workers
    text_dir_str = str(text_dir)

    # Phase 1: Write text files in workers and collect stats
    # This uses mapPartitionsWithIndex to write files and return only stats (not content)
    def write_and_count(partition_idx, records):
        """Write text files in worker and yield stats tuples."""
        text_path = Path(text_dir_str)
        text_path.mkdir(parents=True, exist_ok=True)

        partition_stats = [0, 0, 0]  # text_written, entities_count, errors

        for doc_id, text, entities in records:
            if text:
                text_file = text_path / f"{doc_id}.txt"
                if force or not text_file.exists():
                    try:
                        text_file.write_text(text, encoding='utf-8')
                        partition_stats[0] += 1
                    except Exception as e:
                        logging.error(f"[Partition {partition_idx}] Failed to write text for {doc_id}: {e}")
                        partition_stats[2] += 1

            partition_stats[1] += len(entities)

            # Yield entities for the next phase (without text content)
            for entity in entities:
                if len(entity) >= 4:
                    _, entity_type, value, offsets_json = entity[:4]
                    # Sanitize TSV fields
                    value = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                    offsets_json = str(offsets_json).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                    yield ('entity', doc_id, entity_type, value, offsets_json)

        # Yield partition stats as a special record
        yield ('stats', str(partition_stats[0]), str(partition_stats[1]), str(partition_stats[2]), '')

    # Process all partitions - write text files and extract entities
    processed_rdd = results_rdd.mapPartitionsWithIndex(write_and_count)

    # Separate stats from entities
    stats_rdd = processed_rdd.filter(lambda x: x[0] == 'stats')
    entities_rdd = processed_rdd.filter(lambda x: x[0] == 'entity')

    # Aggregate stats (only small tuples, not content)
    def sum_stats(a, b):
        return ('stats', str(int(a[1]) + int(b[1])), str(int(a[2]) + int(b[2])), str(int(a[3]) + int(b[3])), '')

    # Use fold with initial value to handle empty RDDs
    initial_stats = ('stats', '0', '0', '0', '')

    # Collect stats - this is safe as it's just a few numbers
    stats_collected = stats_rdd.collect()
    if stats_collected:
        total_stats = stats_collected[0]
        for s in stats_collected[1:]:
            total_stats = sum_stats(total_stats, s)
        stats['text_files_written'] = int(total_stats[1])
        stats['entities_written'] = int(total_stats[2])  # This counts entity occurrences
        stats['errors'] = int(total_stats[3])

    # Phase 2: Write entities using Spark DataFrame (distributed write)
    entities_file = entities_dir / 'entities.tsv'

    # Create DataFrame from entities RDD
    schema = StructType([
        StructField("record_type", StringType(), False),
        StructField("doc_id", StringType(), False),
        StructField("type", StringType(), False),
        StructField("value", StringType(), False),
        StructField("offsets_json", StringType(), False),
    ])

    entities_df = spark.createDataFrame(entities_rdd, schema=schema)

    # Write as single TSV file
    tmp_dir = str(entities_file) + ".tmpdir"
    import shutil
    if Path(tmp_dir).exists():
        shutil.rmtree(tmp_dir)

    (
        entities_df.select("doc_id", "type", "value", "offsets_json")
        .coalesce(1)
        .write.mode("overwrite")
        .option("sep", "\t")
        .option("header", "true")
        .option("quote", "\u0000")
        .option("encoding", "UTF-8")
        .csv(tmp_dir)
    )

    # Move part file to final location
    tmp_path = Path(tmp_dir)
    part_files = list(tmp_path.glob("part-*"))
    if part_files:
        part_files[0].rename(entities_file)
        shutil.rmtree(tmp_path, ignore_errors=True)
        # Count entities from file for accurate stats
        with open(entities_file, 'r', encoding='utf-8') as f:
            stats['entities_written'] = sum(1 for _ in f) - 1  # Subtract header

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

    # Create Spark session with optimized configuration for large-scale processing
    spark = SparkSession.builder \
        .appName("HTML-Extractor") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '4g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '2g')) \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .config("spark.default.parallelism", str(args.partitions)) \
        .config("spark.driver.maxResultSize", os.environ.get('SPARK_MAX_RESULT_SIZE', '2g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Create RDD of file paths
        file_paths = [str(f) for f in html_files]
        partitions = min(args.partitions, len(file_paths))
        logger.info(f"Processing with {partitions} partitions")

        files_rdd = spark.sparkContext.parallelize(file_paths, partitions)

        # Process files using mapPartitionsWithIndex
        # NOTE: No caching - data flows through once to avoid memory issues
        results_rdd = files_rdd.mapPartitionsWithIndex(process_html_batch)

        # Write outputs using distributed processing (no collect() on driver)
        logger.info(f"Writing outputs to {output_dir} (distributed mode)")
        stats = write_outputs_distributed(spark, results_rdd, output_dir, args.force)

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