#!/usr/bin/env python3
"""
Spark job for extracting structured data from Wikipedia XML dumps.

Processes Wikipedia dump files (XML or XML.bz2) to extract:
- Pages metadata
- Categories
- Internal links
- Infoboxes
- Abstracts
- Redirect aliases

All outputs are written as TSV files for downstream processing.

Reference: https://www.mediawiki.org/wiki/Help:Export#Export_format
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Add project root to path
sys.path.insert(0, '/opt/app')

from spark.lib.wiki_regexes import (
    extract_page_xml,
    normalize_title,
    extract_categories,
    extract_internal_links,
    extract_infobox_fields,
    extract_abstract,
)
from spark.lib.io import write_tsv, write_ndjson
from spark.lib.utils import StructuredLogger, write_manifest, sha1_hexdigest


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract structured data from Wikipedia dumps"
    )
    parser.add_argument(
        '--wiki-in',
        required=True,
        help='Input directory with Wikipedia dump files'
    )
    parser.add_argument(
        '--out',
        default='workspace/store/wiki',
        help='Output directory for extracted TSV files'
    )
    parser.add_argument(
        '--wiki-max-pages',
        type=int,
        help='Maximum number of pages to process (for development)'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=64,
        help='Number of Spark partitions'
    )
    parser.add_argument(
        '--log',
        default='logs/wiki_extract.jsonl',
        help='Log file path'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List files without processing'
    )
    return parser.parse_args()


def find_wiki_dumps(input_dir: Path) -> List[Path]:
    """Find Wikipedia dump files (XML or XML.bz2)."""
    dump_files = []

    # Look for dump files
    patterns = ['*.xml', '*.xml.bz2']
    for pattern in patterns:
        dump_files.extend(input_dir.glob(pattern))

    # Sort by size (process smaller files first for testing)
    dump_files.sort(key=lambda f: f.stat().st_size)

    return dump_files


def read_dump_streaming(
    spark: SparkSession,
    dump_path: Path,
    max_pages: Optional[int] = None,
    partitions: int = 64
) -> DataFrame:
    """
    Read Wikipedia dump file in streaming fashion.
    Returns DataFrame with page XML strings.
    """
    logging.info(f"Reading dump file: {dump_path}")

    # For compressed files, we'll need special handling
    if dump_path.suffix == '.bz2':
        # Use Spark's built-in compression support
        # Read as text file and process line by line
        rdd = spark.sparkContext.textFile(str(dump_path), minPartitions=partitions)
    else:
        # Plain XML - read as text
        rdd = spark.sparkContext.textFile(str(dump_path), minPartitions=partitions)

    # Process partitions to extract page blocks
    def extract_pages_from_partition(iterator: Iterator[str]) -> Iterator[str]:
        """Extract <page> blocks from lines."""
        buffer = []
        in_page = False
        page_count = 0

        for line in iterator:
            if '<page>' in line:
                in_page = True
                buffer = [line]
            elif in_page:
                buffer.append(line)
                if '</page>' in line:
                    # Complete page found
                    page_xml = '\n'.join(buffer)
                    yield page_xml
                    buffer = []
                    in_page = False
                    page_count += 1

                    # Check max pages limit
                    if max_pages and page_count >= max_pages:
                        break

    # Map partitions to extract pages
    pages_rdd = rdd.mapPartitions(extract_pages_from_partition)

    # Apply global limit if specified
    if max_pages:
        pages_rdd = spark.sparkContext.parallelize(
            pages_rdd.take(max_pages),
            numSlices=min(partitions, max_pages)
        )

    # Convert to DataFrame
    schema = StructType([
        StructField("page_xml", StringType(), False)
    ])

    pages_df = spark.createDataFrame(
        pages_rdd.map(lambda xml: (xml,)),
        schema=schema
    )

    return pages_df


def process_pages(pages_df: DataFrame) -> Tuple[DataFrame, ...]:
    """
    Process page XML to extract structured data.
    Returns tuple of DataFrames: (pages, categories, links, infobox, abstracts)
    """

    # Register UDFs
    extract_page_udf = F.udf(extract_page_xml, MapType(StringType(), StringType()))
    normalize_title_udf = F.udf(normalize_title, StringType())
    extract_categories_udf = F.udf(extract_categories, ArrayType(StringType()))
    extract_links_udf = F.udf(extract_internal_links, ArrayType(StringType()))
    extract_infobox_udf = F.udf(extract_infobox_fields, MapType(StringType(), StringType()))
    extract_abstract_udf = F.udf(extract_abstract, StringType())

    # Extract core fields from XML
    parsed_df = pages_df.withColumn("parsed", extract_page_udf("page_xml"))

    # Filter out failed parses
    parsed_df = parsed_df.filter(F.col("parsed").isNotNull())

    # Extract individual fields
    pages_df = parsed_df.select(
        F.col("parsed.page_id").cast(LongType()).alias("page_id"),
        F.col("parsed.title").alias("title"),
        normalize_title_udf(F.col("parsed.title")).alias("norm_title"),
        F.col("parsed.namespace").cast(IntegerType()).alias("ns"),
        F.col("parsed.redirect_to").alias("redirect_to"),
        F.col("parsed.timestamp").alias("timestamp"),
        F.col("parsed.text").alias("text")
    )

    # Filter to main namespace (ns=0) unless it's a redirect
    pages_df = pages_df.filter(
        (F.col("ns") == 0) | F.col("redirect_to").isNotNull()
    )

    # Create pages metadata (without text)
    pages_meta_df = pages_df.select(
        "page_id", "title", "norm_title", "ns", "redirect_to", "timestamp"
    )

    # Extract categories
    categories_df = pages_df.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        F.explode(extract_categories_udf(F.col("text"))).alias("category")
    ).withColumn(
        "norm_category", normalize_title_udf(F.col("category"))
    )

    # Extract internal links
    links_df = pages_df.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        F.explode(extract_links_udf(F.col("text"))).alias("link_title")
    ).withColumn(
        "norm_link_title", normalize_title_udf(F.col("link_title"))
    )

    # Extract infobox fields
    infobox_df = pages_df.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        extract_infobox_udf(F.col("text")).alias("infobox_map")
    ).select(
        F.col("page_id"),
        F.explode(F.col("infobox_map")).alias("key", "value")
    )

    # Extract abstracts
    abstracts_df = pages_df.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        extract_abstract_udf(F.col("text")).alias("abstract_text")
    ).filter(F.length("abstract_text") > 0)

    # Create aliases from redirects
    aliases_df = pages_df.filter(F.col("redirect_to").isNotNull()).select(
        normalize_title_udf(F.col("title")).alias("alias_norm_title"),
        normalize_title_udf(F.col("redirect_to")).alias("canonical_norm_title")
    )

    return (pages_meta_df, categories_df, links_df, infobox_df, abstracts_df, aliases_df)


def main() -> int:
    """Main entry point."""
    args = parse_args()
    start_time = time.time()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    logger = logging.getLogger(__name__)

    # Setup structured logger
    log_path = Path(args.log)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    struct_logger = StructuredLogger(log_path)
    struct_logger.log("start", wiki_in=args.wiki_in, out=args.out, max_pages=args.wiki_max_pages)

    # Paths
    input_dir = Path(args.wiki_in)
    output_dir = Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find dump files
    dump_files = find_wiki_dumps(input_dir)
    if not dump_files:
        logger.error(f"No Wikipedia dump files found in {input_dir}")
        struct_logger.log("error", message="No dump files found")
        return 1

    logger.info(f"Found {len(dump_files)} dump file(s)")
    dump_file = dump_files[0]  # Process first file for now
    logger.info(f"Processing: {dump_file} ({dump_file.stat().st_size / 1e9:.1f} GB)")

    if args.dry_run:
        logger.info("Dry run mode - listing files only")
        for f in dump_files:
            print(f)
        return 0

    # Create Spark session
    spark = SparkSession.builder \
        .appName("WikiExtractor") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '8g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '4g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read dump file
        pages_df = read_dump_streaming(
            spark, dump_file, args.wiki_max_pages, args.partitions
        )

        # Cache for multiple operations
        pages_df = pages_df.cache()
        page_count = pages_df.count()
        logger.info(f"Loaded {page_count} pages")
        struct_logger.log("pages_loaded", count=page_count)

        # Process pages to extract structured data
        logger.info("Extracting structured data...")
        (pages_meta_df, categories_df, links_df,
         infobox_df, abstracts_df, aliases_df) = process_pages(pages_df)

        # Write outputs
        logger.info("Writing output files...")

        # Pages metadata
        pages_path = output_dir / "pages.tsv"
        write_tsv(pages_meta_df, pages_path,
                  ["page_id", "title", "norm_title", "ns", "redirect_to", "timestamp"],
                  header=True)
        logger.info(f"Wrote pages to {pages_path}")

        # Categories
        categories_path = output_dir / "categories.tsv"
        write_tsv(categories_df, categories_path,
                  ["page_id", "category", "norm_category"],
                  header=True)
        logger.info(f"Wrote categories to {categories_path}")

        # Links
        links_path = output_dir / "links.tsv"
        write_tsv(links_df, links_path,
                  ["page_id", "link_title", "norm_link_title"],
                  header=True)
        logger.info(f"Wrote links to {links_path}")

        # Infobox
        infobox_path = output_dir / "infobox.tsv"
        write_tsv(infobox_df, infobox_path,
                  ["page_id", "key", "value"],
                  header=True)
        logger.info(f"Wrote infobox to {infobox_path}")

        # Abstracts
        abstracts_path = output_dir / "abstract.tsv"
        write_tsv(abstracts_df, abstracts_path,
                  ["page_id", "abstract_text"],
                  header=True)
        logger.info(f"Wrote abstracts to {abstracts_path}")

        # Aliases
        aliases_path = output_dir / "aliases.tsv"
        write_tsv(aliases_df, aliases_path,
                  ["alias_norm_title", "canonical_norm_title"],
                  header=True)
        logger.info(f"Wrote aliases to {aliases_path}")

        # Generate stats
        stats = {
            "pages": pages_meta_df.count(),
            "categories": categories_df.count(),
            "links": links_df.count(),
            "infobox_fields": infobox_df.count(),
            "abstracts": abstracts_df.count(),
            "aliases": aliases_df.count(),
        }

        # Write manifest
        duration = time.time() - start_time
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": round(duration, 2),
            "input": str(dump_file),
            "input_size_gb": round(dump_file.stat().st_size / 1e9, 2),
            "max_pages": args.wiki_max_pages,
            "outputs": {
                "pages": str(pages_path),
                "categories": str(categories_path),
                "links": str(links_path),
                "infobox": str(infobox_path),
                "abstracts": str(abstracts_path),
                "aliases": str(aliases_path),
            },
            "stats": stats,
            "spark_config": {
                "driver_memory": os.environ.get('SPARK_DRIVER_MEMORY', '8g'),
                "partitions": args.partitions,
            }
        }

        runs_dir = Path('runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
        runs_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = runs_dir / 'manifest.json'
        write_manifest(manifest_path, manifest)

        # Log completion
        struct_logger.log("complete", duration=duration, stats=stats)

        # Print summary
        logger.info("=" * 60)
        logger.info("WIKIPEDIA EXTRACTION COMPLETE")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Pages processed: {stats['pages']}")
        logger.info(f"Categories: {stats['categories']}")
        logger.info(f"Links: {stats['links']}")
        logger.info(f"Infobox fields: {stats['infobox_fields']}")
        logger.info(f"Abstracts: {stats['abstracts']}")
        logger.info(f"Aliases: {stats['aliases']}")
        logger.info(f"Manifest: {manifest_path}")
        logger.info("=" * 60)

    finally:
        spark.stop()
        struct_logger.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())