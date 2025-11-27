#!/usr/bin/env python3
"""
PySpark HTML extractor - processes HTML files to extract text and entities.

Uses DataFrame API with mapInPandas for efficient parallel processing.
Optimized for single-host Docker execution with filesystem-based I/O.

Key optimizations:
- DataFrame API instead of RDD for better Catalyst optimization
- mapInPandas for partition-level processing with pandas efficiency
- Single-pass architecture: text + entities written in one pass
- No collect() of large data - only small stats collected
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
from typing import Iterator, List, Optional, Sequence

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

# Add project root to Python path for imports
sys.path.insert(0, '/opt/app')

from config_loader import load_yaml_config
from extractor.config import ExtractorConfig
from extractor.html_clean import html_to_text
from extractor.entity_extractors import extract_all_entities


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description="PySpark HTML to text/entity extractor (DataFrame API)"
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


def discover_html_files(input_dir: Path, limit: Optional[int] = None) -> List[str]:
    """Discover HTML files recursively in input directory"""
    html_files = []

    for root, dirs, files in os.walk(input_dir):
        dirs.sort()
        files.sort()

        for file in files:
            if file.endswith('.html'):
                html_files.append(str(Path(root) / file))

                if limit and len(html_files) >= limit:
                    return html_files[:limit]

    return html_files


def create_file_paths_dataframe(spark: SparkSession, file_paths: List[str], partitions: int) -> DataFrame:
    """
    Create a DataFrame from file paths for parallel processing.

    Uses DataFrame API instead of RDD parallelize for better optimization.
    """
    schema = StructType([
        StructField("file_path", StringType(), False)
    ])

    # Create DataFrame from file paths
    df = spark.createDataFrame(
        [(path,) for path in file_paths],
        schema=schema
    )

    # Repartition for parallel processing
    num_partitions = min(partitions, len(file_paths))
    if num_partitions > 0:
        df = df.repartition(num_partitions)

    return df


def process_html_files_pandas(
    iterator: Iterator[pd.DataFrame],
    text_dir: str,
    entities_parts_dir: str,
    force: bool
) -> Iterator[pd.DataFrame]:
    """
    Process HTML files using pandas - runs on each partition via mapInPandas.

    This function:
    1. Reads HTML content from each file
    2. Extracts text using html_to_text
    3. Extracts entities using extract_all_entities
    4. Writes text files directly
    5. Writes entities to partition-specific TSV
    6. Returns stats DataFrame

    Args:
        iterator: Iterator of pandas DataFrames with 'file_path' column
        text_dir: Directory to write text files
        entities_parts_dir: Directory for partition entity files
        force: Whether to overwrite existing files

    Yields:
        pandas DataFrame with stats (text_written, entity_count, errors)
    """
    import logging
    from pathlib import Path

    # These imports need to be inside the function for worker execution
    sys.path.insert(0, '/opt/app')
    from extractor.html_clean import html_to_text
    from extractor.entity_extractors import extract_all_entities

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    text_path = Path(text_dir)
    text_path.mkdir(parents=True, exist_ok=True)

    # Get partition ID for unique entity file naming
    partition_id = os.getpid()  # Use PID as partition identifier
    entities_file = Path(entities_parts_dir) / f"part-{partition_id}.tsv"

    total_text_written = 0
    total_entity_count = 0
    total_errors = 0

    with open(entities_file, 'w', encoding='utf-8') as ef:
        for pdf in iterator:
            for _, row in pdf.iterrows():
                file_path_str = row['file_path']
                try:
                    file_path = Path(file_path_str)
                    doc_id = file_path.stem

                    # Read HTML content
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        html_content = f.read()

                    if not html_content.strip():
                        continue

                    # Extract text
                    try:
                        text = html_to_text(html_content, strip_boilerplate=False)
                    except Exception as e:
                        logger.error(f"Failed to extract text from {file_path}: {e}")
                        text = ""

                    # Extract entities
                    try:
                        entities = extract_all_entities(doc_id, html_content)
                    except Exception as e:
                        logger.error(f"Failed to extract entities from {file_path}: {e}")
                        entities = []

                    # Write text file
                    if text:
                        text_file = text_path / f"{doc_id}.txt"
                        if force or not text_file.exists():
                            try:
                                text_file.write_text(text, encoding='utf-8')
                                total_text_written += 1
                            except Exception as e:
                                logger.error(f"Failed to write text for {doc_id}: {e}")
                                total_errors += 1

                    # Write entities to partition file
                    for entity in entities:
                        if len(entity) >= 4:
                            _, entity_type, value, offsets_json = entity[:4]
                            value = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                            offsets_json = str(offsets_json).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                            ef.write(f"{doc_id}\t{entity_type}\t{value}\t{offsets_json}\n")
                            total_entity_count += 1

                except Exception as e:
                    logger.error(f"Error processing {file_path_str}: {e}")
                    total_errors += 1

    # Yield stats as DataFrame
    yield pd.DataFrame({
        'text_written': [total_text_written],
        'entity_count': [total_entity_count],
        'errors': [total_errors]
    })


def extract_with_dataframe(
    spark: SparkSession,
    files_df: DataFrame,
    output_dir: Path,
    force: bool = False,
    partitions: int = 64
) -> dict:
    """
    Extract text and entities using DataFrame API with mapInPandas.

    Key advantages over RDD approach:
    1. Catalyst optimizer can optimize the execution plan
    2. Better memory management with DataFrame operations
    3. pandas_udf/mapInPandas provides efficient vectorized operations
    4. No explicit RDD-to-DataFrame conversions needed
    """
    import shutil

    stats = {
        'text_files_written': 0,
        'entities_written': 0,
        'errors': 0
    }

    # Setup output directories
    text_dir = output_dir / 'text'
    entities_dir = output_dir / 'entities'
    entities_parts_dir = entities_dir / '_parts'

    text_dir.mkdir(parents=True, exist_ok=True)
    entities_dir.mkdir(parents=True, exist_ok=True)

    if entities_parts_dir.exists():
        shutil.rmtree(entities_parts_dir)
    entities_parts_dir.mkdir(parents=True, exist_ok=True)

    # Broadcast configuration to workers
    text_dir_str = str(text_dir)
    entities_parts_dir_str = str(entities_parts_dir)
    force_write = force

    # Define output schema for mapInPandas
    stats_schema = StructType([
        StructField("text_written", LongType(), False),
        StructField("entity_count", LongType(), False),
        StructField("errors", LongType(), False)
    ])

    # Create wrapper function that captures the configuration
    def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        return process_html_files_pandas(
            iterator, text_dir_str, entities_parts_dir_str, force_write
        )

    # Process using mapInPandas - this is the DataFrame equivalent of mapPartitions
    stats_df = files_df.mapInPandas(process_partition, schema=stats_schema)

    # Collect only the small stats data (not the actual content)
    stats_rows = stats_df.collect()

    for row in stats_rows:
        stats['text_files_written'] += row['text_written']
        stats['entities_written'] += row['entity_count']
        stats['errors'] += row['errors']

    # Merge partition entity files into final TSV
    entities_file = entities_dir / 'entities.tsv'
    part_files = sorted(entities_parts_dir.glob("part-*.tsv"))

    with open(entities_file, 'w', encoding='utf-8') as out:
        out.write("doc_id\ttype\tvalue\toffsets_json\n")
        for part_file in part_files:
            with open(part_file, 'r', encoding='utf-8') as pf:
                for line in pf:
                    out.write(line)

    # Clean up partition files
    shutil.rmtree(entities_parts_dir, ignore_errors=True)

    return stats


def generate_manifest(
    args: argparse.Namespace,
    stats: dict,
    duration: float,
    file_count: int
) -> dict:
    """Generate run manifest JSON"""
    code_files = list(Path('/opt/app/spark').glob('**/*.py'))
    code_hash = hashlib.sha256()
    for file in sorted(code_files):
        if file.is_file():
            code_hash.update(file.read_bytes())

    manifest = {
        'timestamp': datetime.now().isoformat(),
        'duration_seconds': round(duration, 2),
        'code_hash': code_hash.hexdigest()[:16],
        'api': 'DataFrame',  # Indicate we're using DataFrame API
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

    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    logger = logging.getLogger(__name__)

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

    if args.dry_run:
        logger.info("Dry run mode - listing files only:")
        for i, file in enumerate(html_files[:10], 1):
            print(f"  {i}. {file}")
        if len(html_files) > 10:
            print(f"  ... and {len(html_files) - 10} more")
        return 0

    # Create Spark session with DataFrame-optimized configuration
    spark = SparkSession.builder \
        .appName("HTML-Extractor-DataFrame") \
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
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Create DataFrame from file paths
        partitions = min(args.partitions, len(html_files))
        logger.info(f"Processing with {partitions} partitions using DataFrame API")

        files_df = create_file_paths_dataframe(spark, html_files, partitions)

        # Extract using DataFrame API with mapInPandas
        logger.info(f"Writing outputs to {output_dir} (DataFrame + mapInPandas)")
        stats = extract_with_dataframe(spark, files_df, output_dir, args.force, partitions)

        duration = time.time() - start_time

        # Generate and save manifest
        manifest = generate_manifest(args, stats, duration, len(html_files))

        runs_dir = Path('/opt/app/runs')
        runs_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_dir = runs_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        manifest_file = run_dir / 'manifest.json'
        manifest_file.write_text(json.dumps(manifest, indent=2))

        # Print summary
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE (DataFrame API)")
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
