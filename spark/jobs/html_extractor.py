"""Spark-backed HTML → text/entity extraction job.

This job mirrors the original ``python -m extractor`` behaviour but performs the
CPU-heavy normalization and regex work in parallel workers. It expects to run
inside the Docker Compose cluster defined in ``spark/docker-compose.yml`` so the
host machine only needs Python+venv and Docker.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import SparkSession, types as T

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
EMPTY_STATS = tuple(0 for _ in STAT_FIELDS)


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
    parser = argparse.ArgumentParser(description="Spark-based HTML extractor")
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
        default="spark-html-extractor",
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
    )
    return builder.getOrCreate()


def _sanitize_field(value: str) -> str:
    return value.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def _combine_stats(a: Tuple[int, ...], b: Tuple[int, ...]) -> Tuple[int, ...]:
    return tuple(x + y for x, y in zip(a, b))


def _process_partition(records: Iterable[str], broadcast_conf) -> Iterable[Tuple[Tuple[int, ...], List[EntityRow]]]:
    conf = broadcast_conf.value
    input_root = Path(conf["input_root"])
    text_out = Path(conf["text_out"]) if conf["text_out"] else None
    enable_text = conf["enable_text"]
    enable_entities = conf["enable_entities"]
    force = conf["force"]
    log = logging.getLogger("spark_html_extractor.worker")

    for path_str in records:
        path = Path(path_str)
        doc_id = path.stem

        if (
            not force
            and not enable_entities
            and enable_text
            and text_out is not None
            and text_exists(text_out, path, doc_id, input_root)
        ):
            stats = (0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            yield stats, []
            continue

        try:
            html_content = path.read_text(encoding="utf-8", errors="ignore")
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("Failed to read %s: %s", path, exc)
            yield (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), []
            continue

        if not html_content.strip():
            log.debug("Empty HTML file: %s", path)
            yield (1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0), []
            continue

        text_written = False
        entities: List[EntityRow] = []

        if enable_text and text_out is not None and not conf["dry_run"]:
            try:
                raw_text = html_clean.html_to_text(html_content, strip_boilerplate=False)
                if raw_text:
                    text_written = write_text(text_out, path, doc_id, raw_text, input_root, force=force)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning("Failed to write text for %s: %s", path, exc)

        if enable_entities and not conf["dry_run"]:
            try:
                entities = entity_extractors.extract_all_entities(doc_id, html_content)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning("Failed to extract entities for %s: %s", path, exc)
                entities = []

        entity_types = {row[1] for row in entities}
        stats = (
            1,  # files_processed
            0,  # files_skipped
            int(text_written),
            len(entities),
            int("STAR_COUNT" in entity_types),
            int("FORK_COUNT" in entity_types),
            int("LANG_STATS" in entity_types),
            int("README" in entity_types),
            int("LICENSE" in entity_types),
            int("TOPICS" in entity_types),
            int("URL" in entity_types),
            int("EMAIL" in entity_types),
        )
        yield stats, entities


def _write_entities(spark: SparkSession, entities_rdd, output_path: Path) -> None:
    schema = T.StructType(
        [
            T.StructField("doc_id", T.StringType(), False),
            T.StructField("type", T.StringType(), False),
            T.StructField("value", T.StringType(), False),
            T.StructField("offsets_json", T.StringType(), False),
        ]
    )
    sanitized = entities_rdd.map(
        lambda row: (
            row[0],
            row[1],
            _sanitize_field(row[2]),
            _sanitize_field(row[3]),
        )
    )
    if sanitized.isEmpty():
        df = spark.createDataFrame([], schema)
    else:
        df = spark.createDataFrame(sanitized, schema=schema)
    write_tsv(df, output_path, ["doc_id", "type", "value", "offsets_json"], header=True)


def run_spark_job(spark: SparkSession, html_files: Sequence[Path], opts: JobOptions, logger: logging.Logger) -> Tuple[int, ...]:
    sc = spark.sparkContext
    path_strings = [str(p) for p in html_files]
    partitions = min(max(1, opts.partitions), len(path_strings)) if path_strings else 1
    logger.info("Processing %d HTML files with %d partitions", len(path_strings), partitions)

    broadcast_conf = sc.broadcast(
        {
            "input_root": str(opts.input_root),
            "text_out": str(opts.text_out) if opts.text_out else None,
            "entities_out": str(opts.entities_out) if opts.entities_out else None,
            "enable_text": opts.enable_text,
            "enable_entities": opts.enable_entities,
            "force": opts.force,
            "dry_run": opts.dry_run,
        }
    )

    rdd = sc.parallelize(path_strings, partitions)
    results_rdd = rdd.mapPartitions(lambda records: _process_partition(records, broadcast_conf)).cache()
    stats_rdd = results_rdd.map(lambda pair: pair[0])

    if stats_rdd.isEmpty():
        combined_stats = EMPTY_STATS
    else:
        combined_stats = stats_rdd.fold(EMPTY_STATS, _combine_stats)

    if opts.enable_entities and opts.entities_out:
        entities_rdd = results_rdd.flatMap(lambda pair: pair[1])
        _write_entities(spark, entities_rdd, opts.entities_out)
        logger.info("Wrote entities TSV to %s", opts.entities_out)
    else:
        logger.info("Entity extraction disabled.")

    return combined_stats


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
        stats = run_spark_job(spark, html_files, opts, logger)
    finally:
        spark.stop()

    summary = dict(zip(STAT_FIELDS, stats))
    logger.info("Extraction summary: %s", summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
