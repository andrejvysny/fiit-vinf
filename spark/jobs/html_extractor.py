"""Spark-backed HTML → text/entity extraction job.

Uses DataFrame API with mapInPandas for efficient parallel processing.

This job mirrors the original ``python -m extractor`` behaviour but performs the
CPU-heavy normalization and regex work in parallel workers. It expects to run
inside the Docker Compose cluster defined in ``docker-compose.yml`` so the
host machine only needs Python+venv and Docker.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
)

from config_loader import ConfigError, load_yaml_config
from extractor.config import ExtractorConfig
from extractor.io_utils import HtmlFileDiscovery
from extractor.outputs import text_exists, write_text
from extractor import html_clean, entity_extractors
from spark.lib.io import write_tsv


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
    parser = argparse.ArgumentParser(description="Spark-based HTML extractor (DataFrame API)")
    parser.add_argument("--config", default="config.yml", help="Path to config.yml")
    parser.add_argument(
        "--input-root",
        dest="input_root",
        help="Override extractor.input_root (default: config value).",
    )
    parser.add_argument(
        "--text-out",
        dest="text_out",
        help="Override extractor.outputs.text (default: config value).",
    )
    parser.add_argument(
        "--entities-out",
        dest="entities_out",
        help="Override extractor.outputs.entities (default: config value).",
    )
    parser.add_argument("--limit", type=int, help="Max number of HTML files to process.")
    parser.add_argument("--sample", type=int, help="Alias for --limit.")
    parser.add_argument("--force", action="store_true", help="Reprocess even if text already exists.")
    parser.add_argument("--dry-run", action="store_true", help="List target files without processing.")
    parser.add_argument("--no-text", action="store_true", help="Disable text extraction entirely.")
    parser.add_argument("--no-entities", action="store_true", help="Disable entity extraction.")
    parser.add_argument(
        "--partitions",
        type=int,
        default=64,
        help="Desired Spark partitions for the workload (default: %(default)s).",
    )
    parser.add_argument(
        "--master",
        default=os.environ.get("SPARK_MASTER_URL", "local[*]"),
        help="Spark master URL (default: env SPARK_MASTER_URL or local[*]).",
    )
    parser.add_argument(
        "--app-name",
        default="spark-html-extractor-df",
        help="Spark application name (default: %(default)s).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level for the driver (default: %(default)s).",
    )
    return parser.parse_args(argv)


def _resolve_job_options(args: argparse.Namespace) -> JobOptions:
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

    text_out_value = args.text_out or extractor_cfg.text_out
    text_requested = args.text_out is not None
    enable_text_cfg = extractor_cfg.enable_text if extractor_cfg.enable_text is not None else True
    enable_text = (enable_text_cfg or text_requested) and not args.no_text
    text_out = Path(text_out_value).resolve() if enable_text and text_out_value else None
    if text_out is None:
        enable_text = False

    entities_value = args.entities_out or extractor_cfg.entities_out
    entities_requested = args.entities_out is not None
    enable_entities_cfg = extractor_cfg.enable_entities if extractor_cfg.enable_entities is not None else True
    enable_entities = (enable_entities_cfg or entities_requested) and not args.no_entities
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
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    return logging.getLogger("spark_html_extractor")


def _build_spark_session(opts: JobOptions) -> SparkSession:
    builder = (
        SparkSession.builder.appName(opts.app_name)
        .master(opts.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.maxResultSize", os.environ.get('SPARK_MAX_RESULT_SIZE', '2g'))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.memory.fraction", "0.8")
        .config("spark.memory.storageFraction", "0.3")
    )
    return builder.getOrCreate()


def _sanitize_field(value: str) -> str:
    return value.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def process_partition_pandas(
    iterator: Iterator[pd.DataFrame],
    input_root: str,
    text_out: Optional[str],
    entities_parts_dir: str,
    enable_text: bool,
    enable_entities: bool,
    force: bool,
    dry_run: bool
) -> Iterator[pd.DataFrame]:
    """
    Process HTML files using pandas - runs on each partition via mapInPandas.

    Single-pass architecture:
    1. Read HTML files
    2. Extract text and write directly to text_out
    3. Extract entities and write to partition-specific TSV
    4. Return only stats as DataFrame (no large data to driver)
    """
    log = logging.getLogger("spark_html_extractor.worker")

    input_root_path = Path(input_root)
    text_out_path = Path(text_out) if text_out else None
    entities_parts_path = Path(entities_parts_dir)
    entities_parts_path.mkdir(parents=True, exist_ok=True)

    # Create partition-specific entity file
    partition_id = os.getpid()
    entity_file = entities_parts_path / f"part-{partition_id}.tsv"

    # Initialize stats
    stats = {
        'files_processed': 0,
        'files_skipped': 0,
        'text_written': 0,
        'entities_extracted': 0,
        'stars_found': 0,
        'forks_found': 0,
        'langs_found': 0,
        'readme_found': 0,
        'license_found': 0,
        'topics_found': 0,
        'urls_found': 0,
        'emails_found': 0,
    }

    with open(entity_file, 'w', encoding='utf-8') as ef:
        for pdf in iterator:
            for _, row in pdf.iterrows():
                path_str = row['file_path']
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
                    stats['files_skipped'] += 1
                    continue

                # Read HTML content
                try:
                    html_content = path.read_text(encoding="utf-8", errors="ignore")
                except Exception as exc:
                    log.warning("Failed to read %s: %s", path, exc)
                    stats['files_processed'] += 1
                    continue

                if not html_content.strip():
                    log.debug("Empty HTML file: %s", path)
                    stats['files_processed'] += 1
                    continue

                # Write text in worker
                if enable_text and text_out_path is not None and not dry_run:
                    try:
                        raw_text = html_clean.html_to_text(html_content, strip_boilerplate=False)
                        if raw_text:
                            if write_text(text_out_path, path, doc_id, raw_text, input_root_path, force=force):
                                stats['text_written'] += 1
                    except Exception as exc:
                        log.warning("Failed to write text for %s: %s", path, exc)

                # Extract entities and write to partition file
                if enable_entities and not dry_run:
                    try:
                        entities = entity_extractors.extract_all_entities(doc_id, html_content)
                        entity_types = {row[1] for row in entities}

                        if "STAR_COUNT" in entity_types:
                            stats['stars_found'] += 1
                        if "FORK_COUNT" in entity_types:
                            stats['forks_found'] += 1
                        if "LANG_STATS" in entity_types:
                            stats['langs_found'] += 1
                        if "README" in entity_types:
                            stats['readme_found'] += 1
                        if "LICENSE" in entity_types:
                            stats['license_found'] += 1
                        if "TOPICS" in entity_types:
                            stats['topics_found'] += 1
                        if "URL" in entity_types:
                            stats['urls_found'] += 1
                        if "EMAIL" in entity_types:
                            stats['emails_found'] += 1

                        stats['entities_extracted'] += len(entities)

                        # Write entities to partition file
                        for entity in entities:
                            doc_id_e, type_e, value_e, offsets_e = entity[0], entity[1], entity[2], entity[3]
                            value_e = _sanitize_field(value_e)
                            offsets_e = _sanitize_field(offsets_e)
                            ef.write(f"{doc_id_e}\t{type_e}\t{value_e}\t{offsets_e}\n")

                    except Exception as exc:
                        log.warning("Failed to extract entities for %s: %s", path, exc)

                stats['files_processed'] += 1

    # Yield stats as DataFrame
    yield pd.DataFrame([stats])


def run_spark_job_dataframe(
    spark: SparkSession,
    html_files: Sequence[Path],
    opts: JobOptions,
    logger: logging.Logger
) -> dict:
    """
    Run the Spark extraction job using DataFrame API with mapInPandas.

    Architecture:
    - Create DataFrame from file paths
    - Use mapInPandas for parallel processing
    - Text files written directly in workers
    - Entities written to partition files, merged on driver
    - Only stats collected to driver
    """
    import shutil

    # Create DataFrame from file paths
    path_strings = [str(p) for p in html_files]
    partitions = min(max(1, opts.partitions), len(path_strings)) if path_strings else 1
    logger.info("Processing %d HTML files with %d partitions (DataFrame API)", len(path_strings), partitions)

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
        StructField("files_processed", LongType(), False),
        StructField("files_skipped", LongType(), False),
        StructField("text_written", LongType(), False),
        StructField("entities_extracted", LongType(), False),
        StructField("stars_found", LongType(), False),
        StructField("forks_found", LongType(), False),
        StructField("langs_found", LongType(), False),
        StructField("readme_found", LongType(), False),
        StructField("license_found", LongType(), False),
        StructField("topics_found", LongType(), False),
        StructField("urls_found", LongType(), False),
        StructField("emails_found", LongType(), False),
    ])

    # Create wrapper function that captures configuration
    input_root_str = str(opts.input_root)
    text_out_str = str(opts.text_out) if opts.text_out else None
    entities_parts_str = str(entities_parts_dir) if entities_parts_dir else "/tmp/entities_parts"
    enable_text = opts.enable_text
    enable_entities = opts.enable_entities
    force = opts.force
    dry_run = opts.dry_run

    def process_partition(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        return process_partition_pandas(
            iterator,
            input_root_str,
            text_out_str,
            entities_parts_str,
            enable_text,
            enable_entities,
            force,
            dry_run
        )

    # Process using mapInPandas
    stats_df = files_df.mapInPandas(process_partition, schema=stats_schema)

    # Aggregate stats (collect only small data)
    aggregated = stats_df.agg(
        F.sum("files_processed").alias("files_processed"),
        F.sum("files_skipped").alias("files_skipped"),
        F.sum("text_written").alias("text_written"),
        F.sum("entities_extracted").alias("entities_extracted"),
        F.sum("stars_found").alias("stars_found"),
        F.sum("forks_found").alias("forks_found"),
        F.sum("langs_found").alias("langs_found"),
        F.sum("readme_found").alias("readme_found"),
        F.sum("license_found").alias("license_found"),
        F.sum("topics_found").alias("topics_found"),
        F.sum("urls_found").alias("urls_found"),
        F.sum("emails_found").alias("emails_found"),
    ).collect()[0]

    stats = {field: aggregated[field] for field in STAT_FIELDS}

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


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    opts = _resolve_job_options(args)
    logger = _configure_logging(args.log_level)

    html_files = HtmlFileDiscovery(opts.input_root).discover(limit=opts.limit)
    if not html_files:
        logger.warning("No HTML files found under %s", opts.input_root)
        return 0

    if opts.dry_run:
        logger.info("Dry-run enabled; listing files only.")
        for path in html_files:
            print(path)
        return 0

    spark = _build_spark_session(opts)
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = run_spark_job_dataframe(spark, html_files, opts, logger)
    finally:
        spark.stop()

    logger.info("=" * 60)
    logger.info("EXTRACTION COMPLETE (DataFrame API)")
    logger.info("Extraction summary: %s", stats)
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
