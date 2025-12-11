#!/usr/bin/env python3
"""
Spark job for extracting structured data from Wikipedia XML dumps.

Uses DataFrame API with mapInPandas for efficient parallel processing.

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
import shutil
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    ArrayType, MapType
)
from pyspark import StorageLevel

# Add project root to path
sys.path.insert(0, '/opt/app')

from spark.lib.wiki_regexes import (
    extract_page_xml,
    normalize_title,
    extract_categories,
    extract_internal_links,
    extract_infobox_fields,
    extract_abstract,
    clean_wikitext_to_plaintext,
)
from spark.lib.io import write_tsv, write_ndjson
from spark.lib.stats import PipelineStats, save_pipeline_summary
from spark.lib.utils import StructuredLogger, write_manifest, sha1_hexdigest
from spark.lib.progress import SparkProgressReporter, JobGroup


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract structured data from Wikipedia dumps (DataFrame API)"
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
    parser.add_argument(
        '--prefer-uncompressed',
        action='store_true',
        default=True,
        help='Prefer uncompressed XML over bz2 for better partitioning'
    )
    parser.add_argument(
        '--extract-text',
        action='store_true',
        default=True,
        help='Extract full article text to separate files (default: True)'
    )
    parser.add_argument(
        '--no-text',
        action='store_true',
        help='Disable full text extraction'
    )
    parser.add_argument(
        '--sample-threshold',
        type=int,
        default=50000,
        help='Use Python native streaming for samples below this threshold (default: 50000). '
             'Native mode is memory-efficient and faster for medium-sized extractions.'
    )
    parser.add_argument(
        '--force-spark',
        action='store_true',
        help='Force using Spark even for small samples (not recommended for < 50k pages)'
    )
    parser.add_argument(
        '--coalesce-output',
        action='store_true',
        help='Write single TSV per output (slower and shuffle-heavy for big dumps). Default writes multi-part outputs.'
    )
    parser.add_argument(
        '--max-records-per-file',
        type=int,
        default=2_000_000,
        help='Limit records per output part when not coalescing (helps with very large outputs).'
    )
    return parser.parse_args()


def find_wiki_dumps(input_dir: Path, prefer_uncompressed: bool = True) -> List[Path]:
    """Find Wikipedia dump files (XML or XML.bz2)."""
    dump_files = []

    xml_files = list(input_dir.glob('*.xml'))
    bz2_files = list(input_dir.glob('*.xml.bz2'))

    if prefer_uncompressed and xml_files:
        dump_files = xml_files
    elif bz2_files:
        dump_files = bz2_files
    else:
        dump_files = xml_files + bz2_files

    dump_files.sort(key=lambda f: f.stat().st_size)

    return dump_files


def extract_pages_native(dump_path: Path, max_pages: int, logger: logging.Logger) -> List[str]:
    """
    Extract pages from Wikipedia dump using native Python file reading.

    This is much faster than Spark for small samples because it:
    1. Reads the file sequentially (no shuffle/repartition)
    2. Stops as soon as max_pages is reached
    3. Doesn't load the entire file into memory

    Args:
        dump_path: Path to Wikipedia XML dump file
        max_pages: Maximum number of pages to extract
        logger: Logger instance

    Returns:
        List of page XML strings
    """
    import bz2

    pages = []
    buffer = []
    in_page = False
    max_buffer_lines = 50000

    logger.info(f"Reading {max_pages} pages using native Python (fast mode)...")

    # Open file (handle bz2 compression)
    if dump_path.suffix == '.bz2':
        file_handle = bz2.open(dump_path, 'rt', encoding='utf-8')
    else:
        file_handle = open(dump_path, 'r', encoding='utf-8')

    try:
        for line in file_handle:
            if '<page>' in line:
                in_page = True
                buffer = [line]
            elif in_page:
                buffer.append(line)

                if len(buffer) > max_buffer_lines:
                    # Page too large, skip it
                    buffer = []
                    in_page = False
                    continue

                if '</page>' in line:
                    page_xml = ''.join(buffer)
                    pages.append(page_xml)
                    buffer = []
                    in_page = False

                    if len(pages) % 100 == 0:
                        logger.info(f"Extracted {len(pages)}/{max_pages} pages...")

                    if len(pages) >= max_pages:
                        break
    finally:
        file_handle.close()

    logger.info(f"Extracted {len(pages)} pages using native Python")
    return pages


def process_pages_native(
    pages: List[str],
    output_dir: Path,
    extract_full_text: bool,
    logger: logging.Logger
) -> dict:
    """
    Process extracted pages using pure Python/pandas (no Spark).

    This is used for small sample sizes where Spark overhead is not justified.

    Args:
        pages: List of page XML strings
        output_dir: Output directory for TSV files
        extract_full_text: Whether to extract full article text
        logger: Logger instance

    Returns:
        Dictionary with statistics
    """
    import hashlib

    # Initialize data lists
    pages_data = []
    categories_data = []
    links_data = []
    infobox_data = []
    abstracts_data = []
    aliases_data = []
    text_data = []

    logger.info(f"Processing {len(pages)} pages with Python...")

    for i, page_xml in enumerate(pages):
        # Extract core fields
        parsed = extract_page_xml(page_xml)
        if not parsed:
            continue

        page_id = parsed['page_id']
        title = parsed['title']
        norm_title = normalize_title(title)
        ns = parsed['namespace']
        redirect_to = parsed['redirect_to']
        timestamp = parsed['timestamp']
        text = parsed['text']

        # Filter to main namespace (ns=0) or redirects
        if ns != 0 and redirect_to is None:
            continue

        # Pages metadata
        pages_data.append({
            'page_id': page_id,
            'title': title,
            'norm_title': norm_title,
            'ns': ns,
            'redirect_to': redirect_to or '',
            'timestamp': timestamp or ''
        })

        # Skip content extraction for redirects
        if redirect_to:
            aliases_data.append({
                'alias_norm_title': norm_title,
                'canonical_norm_title': normalize_title(redirect_to)
            })
            continue

        if not text:
            continue

        # Categories
        for category in extract_categories(text):
            categories_data.append({
                'page_id': page_id,
                'category': category,
                'norm_category': normalize_title(category)
            })

        # Links
        for link in extract_internal_links(text):
            links_data.append({
                'page_id': page_id,
                'link_title': link,
                'norm_link_title': normalize_title(link)
            })

        # Infobox
        infobox = extract_infobox_fields(text)
        for key, value in infobox.items():
            infobox_data.append({
                'page_id': page_id,
                'key': key,
                'value': value
            })

        # Abstract
        abstract = extract_abstract(text)
        if abstract:
            abstracts_data.append({
                'page_id': page_id,
                'abstract_text': abstract
            })

        # Full text
        if extract_full_text:
            plain_text = clean_wikitext_to_plaintext(text)
            if plain_text:
                content_sha256 = hashlib.sha256(plain_text.encode('utf-8')).hexdigest()
                text_data.append({
                    'page_id': page_id,
                    'title': title,
                    'content_sha256': content_sha256,
                    'content_length': len(plain_text),
                    'timestamp': timestamp or '',
                    'plain_text': plain_text
                })

        if (i + 1) % 100 == 0:
            logger.info(f"Processed {i + 1}/{len(pages)} pages...")

    logger.info("Writing output files...")

    # Write TSV files using pandas
    def write_tsv_pandas(data: List[dict], path: Path, columns: List[str]):
        # If a previous Spark run produced a directory here (multi-part), clean it
        if path.exists():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

        df = pd.DataFrame(data)
        if df.empty:
            # Create empty file with headers
            df = pd.DataFrame(columns=columns)
        df = df[columns]  # Ensure column order
        df.to_csv(path, sep='\t', index=False, encoding='utf-8')

    # Pages
    pages_path = output_dir / "pages.tsv"
    write_tsv_pandas(pages_data, pages_path,
                     ['page_id', 'title', 'norm_title', 'ns', 'redirect_to', 'timestamp'])
    logger.info(f"Wrote {len(pages_data)} pages to {pages_path}")

    # Categories
    categories_path = output_dir / "categories.tsv"
    write_tsv_pandas(categories_data, categories_path,
                     ['page_id', 'category', 'norm_category'])
    logger.info(f"Wrote {len(categories_data)} categories")

    # Links
    links_path = output_dir / "links.tsv"
    write_tsv_pandas(links_data, links_path,
                     ['page_id', 'link_title', 'norm_link_title'])
    logger.info(f"Wrote {len(links_data)} links")

    # Infobox
    infobox_path = output_dir / "infobox.tsv"
    write_tsv_pandas(infobox_data, infobox_path,
                     ['page_id', 'key', 'value'])
    logger.info(f"Wrote {len(infobox_data)} infobox fields")

    # Abstracts
    abstracts_path = output_dir / "abstract.tsv"
    write_tsv_pandas(abstracts_data, abstracts_path,
                     ['page_id', 'abstract_text'])
    logger.info(f"Wrote {len(abstracts_data)} abstracts")

    # Aliases
    aliases_path = output_dir / "aliases.tsv"
    write_tsv_pandas(aliases_data, aliases_path,
                     ['alias_norm_title', 'canonical_norm_title'])
    logger.info(f"Wrote {len(aliases_data)} aliases")

    # Full text (if enabled)
    text_files_written = 0
    if extract_full_text and text_data:
        text_dir = output_dir / "text"
        text_dir.mkdir(parents=True, exist_ok=True)

        # Deduplicate by SHA256
        seen_hashes = set()
        unique_texts = []
        for item in text_data:
            if item['content_sha256'] not in seen_hashes:
                seen_hashes.add(item['content_sha256'])
                unique_texts.append(item)

        # Write text files
        for item in unique_texts:
            file_path = text_dir / f"{item['content_sha256']}.txt"
            if not file_path.exists():
                file_path.write_text(item['plain_text'], encoding='utf-8')
                text_files_written += 1

        logger.info(f"Wrote {text_files_written} text files")

        # Write metadata
        text_metadata_path = output_dir / "wiki_text_metadata.tsv"
        metadata_items = [{k: v for k, v in item.items() if k != 'plain_text'} for item in text_data]
        write_tsv_pandas(metadata_items, text_metadata_path,
                         ['page_id', 'title', 'content_sha256', 'content_length', 'timestamp'])
        logger.info(f"Wrote text metadata")

    return {
        'pages': len(pages_data),
        'categories': len(categories_data),
        'links': len(links_data),
        'infobox': len(infobox_data),
        'abstracts': len(abstracts_data),
        'aliases': len(aliases_data),
        'text_files_written': text_files_written,
        'text_pages': len(text_data),
        'unique_texts': len(set(item['content_sha256'] for item in text_data)) if text_data else 0
    }


def extract_pages_from_lines_pandas(
    iterator: Iterator[pd.DataFrame],
    max_pages_per_partition: Optional[int] = None
) -> Iterator[pd.DataFrame]:
    """
    Extract <page> blocks from XML lines using pandas - runs via mapInPandas.

    Args:
        iterator: Iterator of pandas DataFrames with 'value' column (lines)
        max_pages_per_partition: Optional limit per partition

    Yields:
        pandas DataFrame with 'page_xml' column
    """
    buffer = []
    in_page = False
    page_count = 0
    max_buffer_lines = 50000
    pages_batch = []
    batch_size = 100  # Yield in batches for efficiency

    for pdf in iterator:
        for line in pdf['value']:
            if '<page>' in line:
                in_page = True
                buffer = [line]
            elif in_page:
                buffer.append(line)

                if len(buffer) > max_buffer_lines:
                    buffer = []
                    in_page = False
                    continue

                if '</page>' in line:
                    page_xml = '\n'.join(buffer)
                    pages_batch.append(page_xml)
                    buffer = []
                    in_page = False
                    page_count += 1

                    # Yield batch
                    if len(pages_batch) >= batch_size:
                        yield pd.DataFrame({'page_xml': pages_batch})
                        pages_batch = []

                    if max_pages_per_partition and page_count >= max_pages_per_partition:
                        break

        if max_pages_per_partition and page_count >= max_pages_per_partition:
            break

    # Yield remaining pages
    if pages_batch:
        yield pd.DataFrame({'page_xml': pages_batch})


def read_dump_as_dataframe(
    spark: SparkSession,
    dump_path: Path,
    max_pages: Optional[int] = None,
    partitions: int = 64
) -> DataFrame:
    """
    Read Wikipedia dump file using DataFrame API.

    Uses spark.read.text() instead of RDD textFile() for better optimization.
    Returns DataFrame with page XML strings.
    """
    logging.info(f"Reading dump file: {dump_path}")

    file_size_gb = dump_path.stat().st_size / 1e9

    # For small sample sizes, fewer partitions is faster (less overhead)
    # Only increase partitions for large files when not sampling
    if file_size_gb > 50 and (max_pages is None or max_pages > 10000):
        partitions = max(256, partitions)
        logging.info(f"Large file detected ({file_size_gb:.1f}GB), using {partitions} partitions")
    else:
        logging.info(f"Using {partitions} partitions for file ({file_size_gb:.1f}GB)")

    # Read using DataFrame API instead of RDD
    if dump_path.suffix == '.bz2':
        logging.warning("Processing compressed file - this may be slower")

    lines_df = spark.read.text(str(dump_path))

    initial_partitions = lines_df.rdd.getNumPartitions()
    target_partitions = partitions

    if max_pages and max_pages <= 10000:
        # For small samples, avoid excessive task counts
        target_partitions = min(partitions, initial_partitions)
        if target_partitions < initial_partitions:
            logging.info(f"Coalescing partitions for small sample: {initial_partitions} -> {target_partitions}")
            lines_df = lines_df.coalesce(target_partitions)
    else:
        # For big runs, avoid shrinking the natural split count (keeps partition size manageable)
        target_partitions = max(partitions, initial_partitions)
        if target_partitions > initial_partitions:
            logging.info(f"Repartitioning for parallelism: {initial_partitions} -> {target_partitions}")
            lines_df = lines_df.repartition(target_partitions)
        else:
            logging.info(f"Keeping reader partitions: {initial_partitions}")

    # Calculate max pages per partition if limit is set
    max_pages_per_partition = None
    if max_pages:
        max_pages_per_partition = max(1, max_pages // target_partitions + 1)

    # Define schema for page extraction output
    page_schema = StructType([
        StructField("page_xml", StringType(), False)
    ])

    # Create wrapper function
    def extract_pages(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        return extract_pages_from_lines_pandas(iterator, max_pages_per_partition)

    # Extract pages using mapInPandas
    pages_df = lines_df.mapInPandas(extract_pages, schema=page_schema)

    # Apply global limit if specified
    if max_pages:
        pages_df = pages_df.limit(max_pages)

    return pages_df


def process_pages_dataframe(pages_df: DataFrame, extract_full_text: bool = True) -> Tuple[DataFrame, ...]:
    """
    Process page XML to extract structured data using DataFrame operations.

    Uses UDFs for parsing (as these are complex operations not available in Spark SQL).
    Returns tuple of DataFrames: (pages, categories, links, infobox, abstracts, aliases, text_metadata?)
    """
    import hashlib

    # Register UDFs for complex parsing operations
    extract_page_udf = F.udf(extract_page_xml, MapType(StringType(), StringType()))
    normalize_title_udf = F.udf(normalize_title, StringType())
    extract_categories_udf = F.udf(extract_categories, ArrayType(StringType()))
    extract_links_udf = F.udf(extract_internal_links, ArrayType(StringType()))
    extract_infobox_udf = F.udf(extract_infobox_fields, MapType(StringType(), StringType()))
    extract_abstract_udf = F.udf(extract_abstract, StringType())
    clean_text_udf = F.udf(clean_wikitext_to_plaintext, StringType())

    def compute_sha256(text: str) -> str:
        if not text:
            return ""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
    sha256_udf = F.udf(compute_sha256, StringType())

    # Extract core fields from XML
    parsed_df = pages_df.withColumn("parsed", extract_page_udf("page_xml"))
    parsed_df = parsed_df.filter(F.col("parsed").isNotNull())

    # Extract individual fields
    pages_with_text = parsed_df.select(
        F.col("parsed.page_id").cast(LongType()).alias("page_id"),
        F.col("parsed.title").alias("title"),
        normalize_title_udf(F.col("parsed.title")).alias("norm_title"),
        F.col("parsed.namespace").cast(IntegerType()).alias("ns"),
        F.col("parsed.redirect_to").alias("redirect_to"),
        F.col("parsed.timestamp").alias("timestamp"),
        F.col("parsed.text").alias("text")
    )

    # Filter to main namespace (ns=0) unless it's a redirect
    pages_with_text = pages_with_text.filter(
        (F.col("ns") == 0) | F.col("redirect_to").isNotNull()
    )

    # 1. Pages metadata
    pages_meta_df = pages_with_text.select(
        "page_id", "title", "norm_title", "ns", "redirect_to", "timestamp"
    )

    # 2. Categories
    categories_df = pages_with_text.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        F.explode(extract_categories_udf(F.col("text"))).alias("category")
    ).withColumn(
        "norm_category", normalize_title_udf(F.col("category"))
    )

    # 3. Internal links
    links_df = pages_with_text.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        F.explode(extract_links_udf(F.col("text"))).alias("link_title")
    ).withColumn(
        "norm_link_title", normalize_title_udf(F.col("link_title"))
    )

    # 4. Infobox fields
    infobox_df = pages_with_text.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        extract_infobox_udf(F.col("text")).alias("infobox_map")
    ).select(
        F.col("page_id"),
        F.explode(F.col("infobox_map")).alias("key", "value")
    )

    # 5. Abstracts
    abstracts_df = pages_with_text.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        extract_abstract_udf(F.col("text")).alias("abstract_text")
    ).filter(F.length("abstract_text") > 0)

    # 6. Aliases from redirects
    aliases_df = pages_with_text.filter(F.col("redirect_to").isNotNull()).select(
        normalize_title_udf(F.col("title")).alias("alias_norm_title"),
        normalize_title_udf(F.col("redirect_to")).alias("canonical_norm_title")
    )

    # 7. Full text extraction (optional)
    if extract_full_text:
        text_metadata_df = pages_with_text.filter(
            (F.col("text").isNotNull()) &
            (F.length(F.col("text")) > 0) &
            (F.col("redirect_to").isNull())
        ).select(
            F.col("page_id"),
            F.col("title"),
            F.col("timestamp"),
            clean_text_udf(F.col("text")).alias("plain_text")
        ).filter(
            F.length(F.col("plain_text")) > 0
        ).withColumn(
            "content_sha256", sha256_udf(F.col("plain_text"))
        ).withColumn(
            "content_length", F.length(F.col("plain_text"))
        ).select(
            "page_id",
            "title",
            "content_sha256",
            "content_length",
            "timestamp",
            "plain_text"
        )

        return (pages_meta_df, categories_df, links_df, infobox_df, abstracts_df, aliases_df, text_metadata_df)
    else:
        return (pages_meta_df, categories_df, links_df, infobox_df, abstracts_df, aliases_df)


def write_text_files_pandas(
    iterator: Iterator[pd.DataFrame],
    text_dir: str
) -> Iterator[pd.DataFrame]:
    """
    Write text files using pandas - runs via mapInPandas.

    Replaces RDD mapPartitions for text file writing.
    """
    from pathlib import Path
    import logging

    log = logging.getLogger("wiki_extractor.text_writer")
    text_path = Path(text_dir)
    text_path.mkdir(parents=True, exist_ok=True)

    written = 0
    for pdf in iterator:
        for _, row in pdf.iterrows():
            sha256 = row['content_sha256']
            content = row['text_content']
            if sha256 and content:
                file_path = text_path / f'{sha256}.txt'
                if not file_path.exists():
                    try:
                        file_path.write_text(content, encoding='utf-8')
                        written += 1
                        if written % 500 == 0:
                            log.info(f"Partition wrote {written} text files so far...")
                    except Exception as e:
                        log.warning(f"Failed to write {sha256}.txt: {e}")

    # Return count as DataFrame
    yield pd.DataFrame({'files_written': [written]})


def main() -> int:
    """Main entry point."""
    args = parse_args()
    start_time = time.time()

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
    dump_files = find_wiki_dumps(input_dir, args.prefer_uncompressed)
    if not dump_files:
        logger.error(f"No Wikipedia dump files found in {input_dir}")
        struct_logger.log("error", message="No dump files found")
        return 1

    logger.info(f"Found {len(dump_files)} dump file(s)")
    dump_file = dump_files[0]
    logger.info(f"Processing: {dump_file} ({dump_file.stat().st_size / 1e9:.1f} GB)")

    if args.dry_run:
        logger.info("Dry run mode - listing files only")
        for f in dump_files:
            print(f)
        return 0

    # Determine if text extraction is enabled
    extract_text = args.extract_text and not args.no_text

    # Use native Python streaming for samples up to threshold (much faster and memory-efficient)
    # Native mode reads XML sequentially and processes incrementally - never loads full data
    use_native = (
        args.wiki_max_pages is not None and
        args.wiki_max_pages <= args.sample_threshold and
        not args.force_spark
    )

    if use_native:
        logger.info("=" * 60)
        logger.info(f"USING NATIVE PYTHON STREAMING MODE (sample <= {args.sample_threshold})")
        logger.info("This mode is memory-efficient: reads XML sequentially, never loads full data")
        logger.info("Processing will stop as soon as max_pages is reached")
        logger.info("=" * 60)

        # Initialize pipeline stats
        pipeline_stats = PipelineStats("wiki_extraction")
        pipeline_stats.set_config(
            mode="native",
            max_pages=args.wiki_max_pages,
            partitions=args.partitions,
            text_extraction=extract_text
        )
        pipeline_stats.set_inputs(
            input_file=str(dump_file),
            input_size_gb=round(dump_file.stat().st_size / 1e9, 2),
            total_items=args.wiki_max_pages
        )

        try:
            # Extract pages using native Python (stops as soon as max_pages reached)
            pages = extract_pages_native(dump_file, args.wiki_max_pages, logger)

            # Process and write outputs using pandas
            stats = process_pages_native(pages, output_dir, extract_text, logger)

            # Generate manifest
            duration = time.time() - start_time
            manifest = {
                "timestamp": datetime.now().isoformat(),
                "duration_seconds": round(duration, 2),
                "api": "Native Python",
                "input": str(dump_file),
                "input_size_gb": round(dump_file.stat().st_size / 1e9, 2),
                "max_pages": args.wiki_max_pages,
                "outputs": {
                    "pages": str(output_dir / "pages.tsv"),
                    "categories": str(output_dir / "categories.tsv"),
                    "links": str(output_dir / "links.tsv"),
                    "infobox": str(output_dir / "infobox.tsv"),
                    "abstracts": str(output_dir / "abstract.tsv"),
                    "aliases": str(output_dir / "aliases.tsv"),
                },
                "stats": stats,
                "features": {
                    "text_extraction": extract_text,
                    "single_file_output": True,
                    "max_records_per_file": args.max_records_per_file
                }
            }

            if extract_text:
                manifest["outputs"]["text_metadata"] = str(output_dir / "wiki_text_metadata.tsv")
                manifest["outputs"]["text_directory"] = str(output_dir / "text")

            runs_dir = Path('runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
            runs_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = runs_dir / 'manifest.json'
            write_manifest(manifest_path, manifest)

            struct_logger.log("complete", duration=duration, stats=stats, mode="native")

            # Save pipeline stats
            pipeline_stats.set_outputs(
                pages_file=str(output_dir / "pages.tsv"),
                categories_file=str(output_dir / "categories.tsv"),
                links_file=str(output_dir / "links.tsv"),
                infobox_file=str(output_dir / "infobox.tsv"),
                abstracts_file=str(output_dir / "abstract.tsv"),
                aliases_file=str(output_dir / "aliases.tsv")
            )
            pipeline_stats.set_entities("pages", stats.get('pages', 0))
            pipeline_stats.set_entities("categories", stats.get('categories', 0))
            pipeline_stats.set_entities("links", stats.get('links', 0))
            pipeline_stats.set_entities("infobox", stats.get('infobox', 0))
            pipeline_stats.set_entities("abstracts", stats.get('abstracts', 0))
            pipeline_stats.set_entities("aliases", stats.get('aliases', 0))
            pipeline_stats.set_entities("text_files", stats.get('text_files_written', 0))
            pipeline_stats.set_nested("performance", "pages_per_second",
                                      round(stats.get('pages', 0) / duration, 2) if duration > 0 else 0)
            pipeline_stats.finalize("completed")
            stats_path = pipeline_stats.save()
            logger.info(f"Stats saved to: {stats_path}")

            # Generate pipeline summary
            try:
                save_pipeline_summary()
            except Exception as e:
                logger.warning(f"Failed to generate pipeline summary: {e}")

            # Print summary
            logger.info("=" * 60)
            logger.info("WIKIPEDIA EXTRACTION COMPLETE (Native Python)")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Pages processed: {stats.get('pages', 'unknown')}")
            logger.info(f"Manifest: {manifest_path}")
            logger.info("=" * 60)

            return 0

        except Exception as e:
            logger.error(f"Native extraction failed: {e}")
            struct_logger.log("error", message=str(e), mode="native")
            pipeline_stats.add_error(str(e), "native_extraction")
            pipeline_stats.finalize("error")
            pipeline_stats.save()
            raise
        finally:
            struct_logger.close()

    # Use Spark for large extractions
    logger.info("Using Spark DataFrame API for extraction")

    # Create Spark session with DataFrame-optimized configuration
    # For large dumps, avoid false executor loss by lengthening heartbeat/network timeouts.
    heartbeat_interval = os.environ.get("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "30s")
    network_timeout = os.environ.get("SPARK_NETWORK_TIMEOUT", "600s")
    spark_builder = SparkSession.builder \
        .appName("WikiExtractor-DataFrame") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '8g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '4g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .config("spark.executor.heartbeatInterval", heartbeat_interval) \
        .config("spark.network.timeout", network_timeout)

    file_size_gb = dump_file.stat().st_size / 1e9
    if file_size_gb > 50:
        spark_builder = spark_builder \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    progress = SparkProgressReporter(spark, logger, interval=45.0)
    logger.info("Starting Spark progress reporter (45s heartbeat) for long-running stages...")
    progress.start("wiki-extraction")

    try:
        # Adjust partitions for small sample sizes to improve performance
        effective_partitions = args.partitions
        if args.wiki_max_pages and args.wiki_max_pages <= 10000:
            # Use fewer partitions for small samples to reduce overhead
            effective_partitions = min(16, args.partitions)
            logger.info(f"Using {effective_partitions} partitions for small sample (max_pages={args.wiki_max_pages})")

        # Read dump file using DataFrame API
        pages_df = read_dump_as_dataframe(
            spark, dump_file, args.wiki_max_pages, effective_partitions
        )

        # Use MEMORY_AND_DISK persistence to avoid OOM errors
        # This spills to disk when memory is full
        # Reference: https://spark.apache.org/docs/latest/tuning.html
        if args.wiki_max_pages:
            logger.info(f"Persisting up to {args.wiki_max_pages} pages with MEMORY_AND_DISK...")
            logger.info("This allows spilling to disk when memory is full (OOM-safe)")
            pages_df = pages_df.persist(StorageLevel.MEMORY_AND_DISK)
            # Note: We don't call count() here to avoid forcing full materialization
            # The data will be materialized lazily during processing

        # Process pages using DataFrame operations
        logger.info("Extracting structured data (DataFrame API)...")
        if extract_text:
            logger.info("Full text extraction ENABLED")
            (pages_meta_df, categories_df, links_df,
             infobox_df, abstracts_df, aliases_df, text_metadata_df) = process_pages_dataframe(pages_df, extract_full_text=True)
        else:
            logger.info("Full text extraction DISABLED")
            (pages_meta_df, categories_df, links_df,
             infobox_df, abstracts_df, aliases_df) = process_pages_dataframe(pages_df, extract_full_text=False)

        # Write outputs - using streaming writes without forcing full materialization
        # Reference: https://spark.apache.org/docs/latest/tuning.html (avoid collect/count)
        logger.info("Writing output files (streaming mode - no full materialization)...")

        # Pages metadata - write directly without count() to avoid OOM
        # Use DISK_ONLY persistence if needed for reuse
        pages_meta_df = pages_meta_df.persist(StorageLevel.DISK_ONLY)

        pages_path = output_dir / "pages.tsv"
        logger.info(
            f"Writing pages.tsv with {pages_meta_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write pages.tsv"):
            write_tsv(
                pages_meta_df,
                pages_path,
                ["page_id", "title", "norm_title", "ns", "redirect_to", "timestamp"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        # Get count after write (data already on disk, safe to count)
        pages_count = pages_meta_df.count()
        logger.info(f"Wrote {pages_count} pages to {pages_path}")

        # Categories
        categories_path = output_dir / "categories.tsv"
        logger.info(
            f"Writing categories.tsv with {categories_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write categories.tsv"):
            write_tsv(
                categories_df,
                categories_path,
                ["page_id", "category", "norm_category"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        logger.info(f"Wrote categories to {categories_path}")

        # Links
        links_path = output_dir / "links.tsv"
        logger.info(
            f"Writing links.tsv with {links_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write links.tsv"):
            write_tsv(
                links_df,
                links_path,
                ["page_id", "link_title", "norm_link_title"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        logger.info(f"Wrote links to {links_path}")

        # Infobox
        infobox_path = output_dir / "infobox.tsv"
        logger.info(
            f"Writing infobox.tsv with {infobox_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write infobox.tsv"):
            write_tsv(
                infobox_df,
                infobox_path,
                ["page_id", "key", "value"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        logger.info(f"Wrote infobox to {infobox_path}")

        # Abstracts
        abstracts_path = output_dir / "abstract.tsv"
        logger.info(
            f"Writing abstract.tsv with {abstracts_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write abstract.tsv"):
            write_tsv(
                abstracts_df,
                abstracts_path,
                ["page_id", "abstract_text"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        logger.info(f"Wrote abstracts to {abstracts_path}")

        # Aliases
        aliases_path = output_dir / "aliases.tsv"
        logger.info(
            f"Writing aliases.tsv with {aliases_df.rdd.getNumPartitions()} partitions "
            f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
        )
        with JobGroup(spark, "Write aliases.tsv"):
            write_tsv(
                aliases_df,
                aliases_path,
                ["alias_norm_title", "canonical_norm_title"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file
            )
        logger.info(f"Wrote aliases to {aliases_path}")

        # Full text extraction (if enabled)
        outputs_written = 6
        text_files_written = 0
        total_text_count = 0
        unique_text_count = 0

        if extract_text:
            logger.info("Writing full text files...")
            text_dir = output_dir / "text"
            text_dir.mkdir(parents=True, exist_ok=True)

            # Use DISK_ONLY persistence for text data (large, used multiple times)
            # This avoids OOM by spilling to disk instead of holding in memory
            text_metadata_df = text_metadata_df.persist(StorageLevel.DISK_ONLY)
            logger.info(
                f"Text metadata partitions: {text_metadata_df.rdd.getNumPartitions()} "
                f"(single_file={args.coalesce_output})"
            )

            # Deduplicate by SHA256 - also persist to disk
            unique_texts_df = text_metadata_df.groupBy("content_sha256").agg(
                F.first("plain_text").alias("text_content"),
                F.first("content_length").alias("length")
            ).persist(StorageLevel.DISK_ONLY)
            logger.info(
                f"Unique text dataframe partitions after grouping: "
                f"{unique_texts_df.rdd.getNumPartitions()}"
            )

            # Define schema for text file writing output
            write_schema = StructType([
                StructField("files_written", LongType(), False)
            ])

            text_dir_str = str(text_dir)

            # Create wrapper function
            def write_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
                return write_text_files_pandas(iterator, text_dir_str)

            # Write using mapInPandas instead of RDD mapPartitions
            with JobGroup(spark, "Write full text contents"):
                write_stats_df = unique_texts_df.mapInPandas(write_partition, schema=write_schema)

            # Collect stats (small data only - just partition counts)
            write_stats = write_stats_df.agg(F.sum("files_written")).collect()
            text_files_written = write_stats[0][0] if write_stats else 0

            logger.info(f"Wrote {text_files_written} unique text files to {text_dir}")

            # Write metadata TSV
            text_metadata_path = output_dir / "wiki_text_metadata.tsv"
            logger.info(
                f"Writing wiki_text_metadata.tsv with {text_metadata_df.rdd.getNumPartitions()} partitions "
                f"(single_file={args.coalesce_output}, max_records_per_file={args.max_records_per_file})"
            )
            with JobGroup(spark, "Write text metadata"):
                write_tsv(
                    text_metadata_df.select("page_id", "title", "content_sha256", "content_length", "timestamp"),
                    text_metadata_path,
                    ["page_id", "title", "content_sha256", "content_length", "timestamp"],
                    header=True,
                    single_file=args.coalesce_output,
                    max_records_per_file=args.max_records_per_file
                )

            # Get counts AFTER writing (data is on disk now, safe to count)
            total_text_count = text_metadata_df.count()
            unique_text_count = unique_texts_df.count()
            logger.info(f"Wrote text metadata to {text_metadata_path}")
            logger.info(f"Text extraction stats: {total_text_count} pages, {unique_text_count} unique content ({text_files_written} files written)")

            outputs_written = 8

        # Generate stats
        stats = {
            "pages": pages_count,
            "outputs_written": outputs_written,
            "files_processed": 1,
        }

        if extract_text:
            stats["text_files_written"] = text_files_written
            stats["text_pages"] = total_text_count
            stats["unique_texts"] = unique_text_count

        # Write manifest
        duration = time.time() - start_time
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": round(duration, 2),
            "api": "DataFrame",
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
            },
            "features": {
                "text_extraction": extract_text,
                "single_file_output": args.coalesce_output,
                "max_records_per_file": args.max_records_per_file
            }
        }

        if extract_text:
            manifest["outputs"]["text_metadata"] = str(text_metadata_path)
            manifest["outputs"]["text_directory"] = str(text_dir)

        runs_dir = Path('runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
        runs_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = runs_dir / 'manifest.json'
        write_manifest(manifest_path, manifest)

        struct_logger.log("complete", duration=duration, stats=stats)

        # Clean up persisted DataFrames to free disk/memory
        logger.info("Cleaning up persisted data...")
        try:
            pages_df.unpersist()
            pages_meta_df.unpersist()
            if extract_text:
                text_metadata_df.unpersist()
                unique_texts_df.unpersist()
        except Exception as cleanup_err:
            logger.warning(f"Cleanup warning (non-fatal): {cleanup_err}")

        # Print summary
        logger.info("=" * 60)
        logger.info("WIKIPEDIA EXTRACTION COMPLETE (DataFrame API)")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Pages processed: {stats.get('pages', 'unknown')}")
        logger.info(f"Outputs written: {stats['outputs_written']}")
        logger.info(f"Manifest: {manifest_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        struct_logger.log("error", message=str(e))
        raise
    finally:
        progress.stop()
        spark.stop()
        struct_logger.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
