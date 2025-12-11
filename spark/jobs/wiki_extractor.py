#!/usr/bin/env python3
"""
Spark job for extracting structured data from Wikipedia XML dumps.

Uses DataFrame API with mapPartitions for efficient parallel processing, configured via environment variables.

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

import json
import logging
import os
import sys
import time
import shutil
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator, List, Optional, Tuple

from pyspark.sql import Column, DataFrame, Row, SparkSession
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
from spark.lib.io import write_tsv, write_ndjson, merge_part_files
from spark.lib.stats import PipelineStats, save_pipeline_summary
from spark.lib.utils import StructuredLogger, write_manifest, sha1_hexdigest
from spark.lib.progress import SparkProgressReporter, JobGroup


def _bool_env(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: Optional[int] = None) -> Optional[int]:
    val = os.environ.get(name)
    if val is None or val == "":
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _load_env_args() -> SimpleNamespace:
    """Load runtime options from environment variables."""
    wiki_max_pages = _int_env("WIKI_MAX_PAGES", _int_env("SAMPLE"))
    return SimpleNamespace(
        wiki_in=os.environ.get("WIKI_IN", "/opt/app/wiki_dump"),
        out=os.environ.get("WIKI_OUT", "/opt/app/workspace/store/wiki"),
        wiki_max_pages=wiki_max_pages,
        partitions=_int_env("WIKI_PARTITIONS", _int_env("PARTITIONS", 64)) or 64,
        log=os.environ.get("WIKI_LOG", "logs/wiki_extract.jsonl"),
        dry_run=_bool_env("WIKI_DRY_RUN", False),
        prefer_uncompressed=_bool_env("WIKI_PREFER_UNCOMPRESSED", True),
        extract_text=_bool_env("WIKI_EXTRACT_TEXT", True),
        no_text=_bool_env("WIKI_NO_TEXT", False),
        coalesce_output=_bool_env("WIKI_COALESCE_OUTPUT", False),
        merge_output=_bool_env("WIKI_MERGE_OUTPUT", True),
        no_merge=_bool_env("WIKI_NO_MERGE", False),
        max_records_per_file=_int_env("WIKI_MAX_RECORDS_PER_FILE", 2_000_000) or 2_000_000,
        stage_dir=os.environ.get("WIKI_STAGE_DIR"),
        force=_bool_env("WIKI_FORCE", False),
    )


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


def extract_pages_from_lines_partition(
    iterator: Iterator[Row],
    max_pages_per_partition: Optional[int] = None
) -> Iterator[Row]:
    """
    Extract <page> blocks from XML lines - runs via mapPartitions.

    Args:
        iterator: Iterator of Row objects with 'value' field (lines)
        max_pages_per_partition: Optional limit per partition

    Yields:
        Row with 'page_xml' field
    """
    buffer = []
    in_page = False
    page_count = 0
    max_buffer_lines = 50000

    for row in iterator:
        line = row['value']
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
                yield Row(page_xml=page_xml)
                buffer = []
                in_page = False
                page_count += 1

                if max_pages_per_partition and page_count >= max_pages_per_partition:
                    return


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

    # Create wrapper function for mapPartitions
    def extract_pages(iterator: Iterator[Row]) -> Iterator[Row]:
        return extract_pages_from_lines_partition(iterator, max_pages_per_partition)

    # Extract pages using mapPartitions on RDD, then convert to DataFrame
    pages_rdd = lines_df.rdd.mapPartitions(extract_pages)
    pages_df = spark.createDataFrame(pages_rdd, schema=page_schema)

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
    log = logging.getLogger(__name__)

    log.info("Registering UDFs for XML parsing...")
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

    # CRITICAL: Persist pages_with_text to avoid re-running extract_page_udf
    # for each of the 6 output DataFrames. Without this, the expensive XML parsing
    # UDF runs 6 times per page (36M calls instead of 6M for full dump).
    # Use DISK_ONLY to stream through disk without RAM pressure.
    log.info("Persisting parsed pages (pages_with_text) with DISK_ONLY...")
    log.info("This avoids re-running XML parsing UDF for each of the 6 output DataFrames")
    pages_with_text = pages_with_text.persist(StorageLevel.DISK_ONLY)

    log.info("Building lazy DataFrame transformations for 6 output types...")
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

    def _single_line(col: Column) -> Column:
        """
        Force text to a single line suitable for TSV output by removing tabs,
        newlines, and NULs, then collapsing whitespace.
        """
        cleaned = F.regexp_replace(col, r"[\r\n\t\x00\x0b\x0c\u2028\u2029]+", " ")
        cleaned = F.translate(cleaned, '"', "'")
        cleaned = F.regexp_replace(cleaned, r"\s+", " ")
        return F.trim(cleaned)

    # 5. Abstracts (sanitized for TSV)
    abstracts_df = pages_with_text.filter(F.col("text").isNotNull()).select(
        F.col("page_id"),
        extract_abstract_udf(F.col("text")).alias("abstract_text")
    ).withColumn(
        "abstract_text", _single_line(F.col("abstract_text"))
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


def write_text_files_partition(
    iterator: Iterator[Row],
    text_dir: str
) -> Iterator[Row]:
    """
    Write text files using native Python - runs via mapPartitions.

    Uses native file I/O instead of pandas.
    """
    from pathlib import Path
    import logging

    log = logging.getLogger("wiki_extractor.text_writer")
    text_path = Path(text_dir)
    text_path.mkdir(parents=True, exist_ok=True)

    written = 0
    for row in iterator:
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

    # Return count as Row
    yield Row(files_written=written)


def main() -> int:
    """Main entry point."""
    args = _load_env_args()
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
    stage_dir = Path(args.stage_dir) if args.stage_dir else output_dir

    def clean_dir_contents(path: Path) -> None:
        if not path.exists():
            return
        for child in path.iterdir():
            try:
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            except Exception as cleanup_err:
                logger.warning(f"Force cleanup warning for {child}: {cleanup_err}")

    if args.force:
        clean_dir_contents(output_dir)
        if stage_dir != output_dir:
            clean_dir_contents(stage_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    stage_dir.mkdir(parents=True, exist_ok=True)

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

    # Use Spark for all extractions
    logger.info("Using Spark DataFrame API for extraction")

    is_full_extraction = args.wiki_max_pages is None

    # Determine output merge strategy:
    # Default: merge (write parts in parallel, then merge into single files)
    # This is the recommended approach per Spark docs - avoids coalesce(1) bottleneck
    # User can disable with --no-merge to get directory with part files
    use_merge = args.merge_output and not args.no_merge
    if not use_merge:
        logger.info("Output mode: PARTITIONED (directory with part files)")
    else:
        logger.info("Output mode: MERGE (write parts in parallel, then merge into single files)")
        logger.info("Reference: https://spark.apache.org/docs/latest/sql-data-sources-csv.html")

    # Create Spark session with DataFrame-optimized configuration
    # For large dumps, avoid false executor loss by lengthening heartbeat/network timeouts.
    heartbeat_interval = os.environ.get("SPARK_EXECUTOR_HEARTBEAT_INTERVAL", "30s")
    network_timeout = os.environ.get("SPARK_NETWORK_TIMEOUT", "600s")

    # Python worker memory - critical for UDF-heavy workloads
    # Default 512m is too low for XML parsing UDFs processing large partitions
    python_worker_memory = os.environ.get("SPARK_PYTHON_WORKER_MEMORY", "1g")

    spark_builder = SparkSession.builder \
        .appName("WikiExtractor-DataFrame") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '8g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '4g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false" if is_full_extraction else "true") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .config("spark.executor.heartbeatInterval", heartbeat_interval) \
        .config("spark.network.timeout", network_timeout) \
        .config("spark.python.worker.memory", python_worker_memory) \
        .config("spark.python.worker.reuse", "true")

    file_size_gb = dump_file.stat().st_size / 1e9
    if file_size_gb > 50:
        # For large files: optimize memory and disable features that can cause OOM
        logger.info(f"Large file detected ({file_size_gb:.1f}GB) - applying memory optimizations")
        spark_builder = spark_builder \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.3") \
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Use shorter progress interval for full extraction (can take hours)
    progress_interval = 30.0 if is_full_extraction else 45.0
    progress = SparkProgressReporter(spark, logger, interval=progress_interval)
    if is_full_extraction:
        logger.info("=" * 60)
        logger.info("FULL WIKIPEDIA EXTRACTION - This may take several hours!")
        logger.info(f"Input: {dump_file} ({dump_file.stat().st_size / 1e9:.1f} GB)")
        logger.info("Progress will be logged every 30 seconds")
        logger.info("=" * 60)
    else:
        logger.info(f"Starting progress reporter ({progress_interval}s interval)...")
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

        # CRITICAL FIX: Always persist pages_df to avoid re-scanning source file
        # Without persistence, each output DataFrame triggers a FULL re-read of source:
        # - 111.5GB × 6 outputs = 670GB+ I/O for full extraction!
        # - Plus 6M × 6 = 36M redundant UDF calls!
        #
        # Use DISK_ONLY to avoid RAM pressure - all data streams through disk.
        # Reference: https://spark.apache.org/docs/latest/tuning.html
        run_desc = f"{args.wiki_max_pages} pages" if args.wiki_max_pages else "FULL dump (~6M pages)"
        logger.info(f"Persisting extracted pages ({run_desc}) with DISK_ONLY...")
        logger.info("Using disk-only streaming to avoid RAM pressure on large datasets")
        pages_df = pages_df.persist(StorageLevel.DISK_ONLY)

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
        # Note: pages_df and pages_with_text are already persisted with DISK_ONLY
        # Reference: https://spark.apache.org/docs/latest/tuning.html (avoid collect/count)
        logger.info("Writing output files (streaming from DISK_ONLY cache)...")
        logger.info("Each output will read from persisted pages_with_text, not re-scan source")

        # Pages metadata - persist separately for count() after write
        pages_meta_df = pages_meta_df.persist(StorageLevel.DISK_ONLY)

        # Cap output partitions - but NOT for full extraction to avoid memory pressure
        # Coalescing causes each output task to process multiple input partitions,
        # which can cause Python worker OOM on large datasets
        def cap_partitions(df: DataFrame, cap: int = 128) -> DataFrame:
            if is_full_extraction:
                return df
            current = df.rdd.getNumPartitions()
            target = min(cap, current)
            if target < current:
                logger.info(f"Coalescing output partitions: {current} -> {target}")
                return df.coalesce(target)
            return df

        # Coalesce all outputs to a manageable number of files
        pages_meta_df = cap_partitions(pages_meta_df)

        pages_path = stage_dir / "pages.tsv"
        logger.info(
            f"Writing pages.tsv with {pages_meta_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write pages.tsv"):
            write_tsv(
                pages_meta_df,
                pages_path,
                ["page_id", "title", "norm_title", "ns", "redirect_to", "timestamp"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
            )
        # Get count after write (data already on disk, safe to count)
        pages_count = pages_meta_df.count()
        logger.info(f"Wrote {pages_count} pages to {pages_path}")

        # Categories
        categories_path = stage_dir / "categories.tsv"
        categories_df = cap_partitions(categories_df)
        logger.info(
            f"Writing categories.tsv with {categories_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write categories.tsv"):
            write_tsv(
                categories_df,
                categories_path,
                ["page_id", "category", "norm_category"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
            )
        logger.info(f"Wrote categories to {categories_path}")

        # Links
        links_path = stage_dir / "links.tsv"
        links_df = cap_partitions(links_df)
        logger.info(
            f"Writing links.tsv with {links_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write links.tsv"):
            write_tsv(
                links_df,
                links_path,
                ["page_id", "link_title", "norm_link_title"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
            )
        logger.info(f"Wrote links to {links_path}")

        # Infobox
        infobox_path = stage_dir / "infobox.tsv"
        infobox_df = cap_partitions(infobox_df)
        logger.info(
            f"Writing infobox.tsv with {infobox_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write infobox.tsv"):
            write_tsv(
                infobox_df,
                infobox_path,
                ["page_id", "key", "value"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
            )
        logger.info(f"Wrote infobox to {infobox_path}")

        # Abstracts
        abstracts_path = stage_dir / "abstract.tsv"
        abstracts_df = cap_partitions(abstracts_df)
        logger.info(
            f"Writing abstract.tsv with {abstracts_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write abstract.tsv"):
            write_tsv(
                abstracts_df,
                abstracts_path,
                ["page_id", "abstract_text"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
            )
        logger.info(f"Wrote abstracts to {abstracts_path}")

        # Aliases
        aliases_path = stage_dir / "aliases.tsv"
        aliases_df = cap_partitions(aliases_df)
        logger.info(
            f"Writing aliases.tsv with {aliases_df.rdd.getNumPartitions()} partitions "
            f"(merge={use_merge}, single_file={args.coalesce_output})"
        )
        with JobGroup(spark, "Write aliases.tsv"):
            write_tsv(
                aliases_df,
                aliases_path,
                ["alias_norm_title", "canonical_norm_title"],
                header=True,
                single_file=args.coalesce_output,
                max_records_per_file=args.max_records_per_file,
                merge_after_write=use_merge
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

            # Create wrapper function for mapPartitions
            def write_partition(iterator: Iterator[Row]) -> Iterator[Row]:
                return write_text_files_partition(iterator, text_dir_str)

            # Write using mapPartitions on RDD, then convert to DataFrame
            with JobGroup(spark, "Write full text contents"):
                write_stats_rdd = unique_texts_df.rdd.mapPartitions(write_partition)
                write_stats_df = spark.createDataFrame(write_stats_rdd, schema=write_schema)

            # Collect stats (small data only - just partition counts)
            write_stats = write_stats_df.agg(F.sum("files_written")).collect()
            text_files_written = write_stats[0][0] if write_stats else 0

            logger.info(f"Wrote {text_files_written} unique text files to {text_dir}")

            # Write metadata TSV
            text_metadata_path = output_dir / "wiki_text_metadata.tsv"
            logger.info(
                f"Writing wiki_text_metadata.tsv with {text_metadata_df.rdd.getNumPartitions()} partitions "
                f"(merge={use_merge}, single_file={args.coalesce_output})"
            )
            with JobGroup(spark, "Write text metadata"):
                write_tsv(
                    text_metadata_df.select("page_id", "title", "content_sha256", "content_length", "timestamp"),
                    text_metadata_path,
                    ["page_id", "title", "content_sha256", "content_length", "timestamp"],
                    header=True,
                    single_file=args.coalesce_output,
                    max_records_per_file=args.max_records_per_file,
                    merge_after_write=use_merge
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
                "pages": str(output_dir / "pages.tsv"),
                "categories": str(output_dir / "categories.tsv"),
                "links": str(output_dir / "links.tsv"),
                "infobox": str(output_dir / "infobox.tsv"),
                "abstracts": str(output_dir / "abstract.tsv"),
                "aliases": str(output_dir / "aliases.tsv"),
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
            manifest["outputs"]["text_metadata"] = str(output_dir / "wiki_text_metadata.tsv")
            manifest["outputs"]["text_directory"] = str(output_dir / "text")

        runs_dir = Path('runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
        runs_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = runs_dir / 'manifest.json'
        write_manifest(manifest_path, manifest)

        struct_logger.log("complete", duration=duration, stats=stats)

        # If staging was used, copy outputs to the final output_dir (host mount) once.
        if stage_dir != output_dir:
            logger.info(f"Staged output in {stage_dir}, copying to final {output_dir} ...")
            clean_dir_contents(output_dir)
            for item in stage_dir.iterdir():
                target = output_dir / item.name
                if item.is_dir():
                    shutil.copytree(item, target)
                else:
                    shutil.copy2(item, target)
            logger.info("Copy from staging to final output complete.")

        # Clean up persisted DataFrames to free disk/memory
        logger.info("Cleaning up persisted data...")
        try:
            pages_df.unpersist()
            pages_meta_df.unpersist()
            if 'pages_with_text' in locals():
                pages_with_text.unpersist()
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
