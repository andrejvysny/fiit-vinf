#!/usr/bin/env python3
"""
Spark job for joining HTML entities with Wikipedia canonical data.

Uses pure DataFrame API for all operations (no RDD usage).

Joins entities extracted from HTML (GitHub pages) with Wikipedia pages based on
normalized entity values. Handles aliases, calculates confidence scores, and
resolves collisions.

Supported entity types:
- LANG/LANG_STATS: Programming languages
- LICENSE: Software licenses
- TOPICS: Repository topics
- README: Keywords from README files
"""

import json
import logging
import os
import sys
import time
import re
from urllib.parse import urlparse, unquote
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import StorageLevel

# Add project root to path
sys.path.insert(0, '/opt/app')

from spark.lib.wiki_regexes import normalize_title
from spark.lib.io import read_tsv, write_tsv, write_ndjson
from spark.lib.utils import StructuredLogger, write_manifest
from spark.lib.stats import PipelineStats, save_pipeline_summary


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


def _resolve_default_entities_path() -> str:
    """
    Prefer the heaviest, actually populated entities dump.
    Falls back to legacy location if the primary file is missing.
    """
    candidates = [
        "/opt/app/workspace/store/entities/entities2.tsv",
        "/opt/app/workspace/store/entities/entities.tsv",
        "/opt/app/workspace/store/spark/entities/entities.tsv",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
    return candidates[-1]


def _load_env_args() -> SimpleNamespace:
    """Load runtime options from environment variables."""
    entities_max_rows = _int_env("JOIN_ENTITIES_MAX_ROWS", _int_env("SAMPLE"))
    return SimpleNamespace(
        entities=os.environ.get("JOIN_ENTITIES", _resolve_default_entities_path()),
        wiki=os.environ.get("JOIN_WIKI", "/opt/app/workspace/store/spark/wiki"),
        out=os.environ.get("JOIN_OUT", "/opt/app/workspace/store/spark/join"),
        entities_max_rows=entities_max_rows,
        partitions=_int_env("JOIN_PARTITIONS", _int_env("PARTITIONS", 64)) or 64,
        log=os.environ.get("JOIN_LOG", "logs/wiki_join.jsonl"),
        stats_enabled=_bool_env("JOIN_STATS", True),
        dry_run=_bool_env("JOIN_DRY_RUN", False),
        coalesce_output=_bool_env("JOIN_COALESCE_OUTPUT", False),
        max_records_per_file=_int_env("JOIN_MAX_RECORDS_PER_FILE", 2_000_000) or 2_000_000,
    )


def load_entities(spark: SparkSession, path: Path, max_rows: Optional[int] = None) -> DataFrame:
    """Load entities TSV with optional row limit."""
    entities_df = read_tsv(spark, path, header=True)

    # Apply row limit if specified
    if max_rows:
        entities_df = entities_df.limit(max_rows)

    return entities_df


def load_wiki_data(spark: SparkSession, wiki_dir: Path, logger) -> Dict[str, DataFrame]:
    """
    Load Wikipedia TSV files using lazy evaluation.

    STREAMING OPTIMIZATION: Does NOT call count() during load to avoid full scans.
    Data is loaded lazily and only materialized during the join operation.
    Reference: https://spark.apache.org/docs/latest/tuning.html
    """
    wiki_data = {}

    # Load pages - NO count() to avoid full scan
    pages_path = wiki_dir / "pages.tsv"
    if pages_path.exists():
        wiki_data['pages'] = read_tsv(spark, pages_path, header=True)
        logger.info(f"Loaded pages from {pages_path} (lazy evaluation)")

    # Load aliases - NO count() to avoid full scan
    aliases_path = wiki_dir / "aliases.tsv"
    if aliases_path.exists():
        wiki_data['aliases'] = read_tsv(spark, aliases_path, header=True)
        logger.info(f"Loaded aliases from {aliases_path} (lazy evaluation)")

    # Load categories - NO count() to avoid full scan
    categories_path = wiki_dir / "categories.tsv"
    if categories_path.exists():
        wiki_data['categories'] = read_tsv(spark, categories_path, header=True)
        logger.info(f"Loaded categories from {categories_path} (lazy evaluation)")

    # Load abstracts - NO count() to avoid full scan
    abstracts_path = wiki_dir / "abstract.tsv"
    if abstracts_path.exists():
        wiki_data['abstracts'] = read_tsv(spark, abstracts_path, header=True)
        logger.info(f"Loaded abstracts from {abstracts_path} (lazy evaluation)")

    # Load infobox (optional) - NO count()
    infobox_path = wiki_dir / "infobox.tsv"
    if infobox_path.exists():
        wiki_data['infobox'] = read_tsv(spark, infobox_path, header=True)
        logger.info(f"Loaded infobox from {infobox_path} (lazy evaluation)")

    return wiki_data


def prepare_canonical_mapping(pages_df: DataFrame, aliases_df: Optional[DataFrame]) -> DataFrame:
    """
    Create canonical mapping from normalized titles to page info.
    Includes both direct pages and aliases.
    """
    logger = logging.getLogger(__name__)

    def _compact(col: str):
        return F.regexp_replace(F.coalesce(F.col(col), F.lit("")), r"[\s_.-]+", "")

    normalize_title_udf = F.udf(normalize_title, StringType())

    # Restrict to main namespace; keep redirects for aliasing
    mainspace_pages = pages_df.filter(F.col("ns") == 0)
    canonical_pages = mainspace_pages.filter(F.col("redirect_to").isNull())

    # Create canonical entries from pages
    canonical_df = canonical_pages.select(
        F.col("norm_title").alias("norm_key"),
        _compact("norm_title").alias("norm_key_compact"),
        F.col("page_id"),
        F.col("title").alias("wiki_title"),
        F.lit("direct").alias("match_type")
    )

    # If we have aliases, join them to get canonical pages
    if aliases_df is not None:
        # Join aliases with pages to get canonical page info
        alias_canonical = aliases_df.alias("a").join(
            canonical_pages.alias("p"),
            F.col("a.canonical_norm_title") == F.col("p.norm_title"),
            "inner"
        ).select(
            F.col("a.alias_norm_title").alias("norm_key"),
            _compact("a.alias_norm_title").alias("norm_key_compact"),
            F.col("p.page_id"),
            F.col("p.title").alias("wiki_title"),
            F.lit("alias").alias("match_type")
        )

        # Union direct and alias mappings
        canonical_df = canonical_df.union(alias_canonical)

    # Treat redirects as aliases to boost coverage
    redirects_df = mainspace_pages.filter(F.col("redirect_to").isNotNull())
    if redirects_df is not None:
        redirects_enriched = redirects_df.withColumn(
            "redirect_norm_title",
            normalize_title_udf(F.col("redirect_to"))
        )

        redirects_canonical = redirects_enriched.alias("r").join(
            canonical_pages.alias("p"),
            F.col("r.redirect_norm_title") == F.col("p.norm_title"),
            "inner"
        ).select(
            F.col("r.norm_title").alias("norm_key"),
            _compact("r.norm_title").alias("norm_key_compact"),
            F.col("p.page_id"),
            F.col("p.title").alias("wiki_title"),
            F.lit("redirect").alias("match_type")
        )
        canonical_df = canonical_df.union(redirects_canonical)

    # Deduplicate (prefer direct matches over aliases)
    match_priority = F.when(F.col("match_type") == "direct", F.lit(0)) \
                      .when(F.col("match_type") == "redirect", F.lit(1)) \
                      .otherwise(F.lit(2))

    window = Window.partitionBy("norm_key").orderBy(
        match_priority,
        F.col("page_id")
    )
    canonical_df = canonical_df.withColumn(
        "rank", F.row_number().over(window)
    ).filter(F.col("rank") == 1).drop("rank")

    # Also deduplicate by compact key to avoid collisions like "type script" vs "typescript"
    compact_window = Window.partitionBy("norm_key_compact").orderBy(
        match_priority,
        F.col("page_id")
    )
    canonical_df = canonical_df.withColumn(
        "rank", F.row_number().over(compact_window)
    ).filter(F.col("rank") == 1).drop("rank")

    canonical_df = canonical_df.filter(F.col("norm_key") != "")

    logger.info("Canonical mapping prepared with compact keys and redirect aliases")

    return canonical_df


def get_supported_entity_types() -> List[str]:
    """Return list of entity types we support for joining."""
    return [
        'LANG_STATS',     # Programming language statistics
        'LICENSE',        # Software licenses
        'TOPICS',         # GitHub topics
        'README',         # Keywords from README
        'URL',            # Could be used for external links
    ]


def normalize_entity_value(value: str, entity_type: str) -> str:
    """
    Normalize entity value for matching with Wikipedia titles.
    Different normalization strategies per entity type.
    """
    if not value:
        return ""

    value = value.strip()

    # License shorthands to canonical labels before normalization
    license_map = {
        "mit": "MIT License",
        "mit license": "MIT License",
        "mit-license": "MIT License",
        "apache-2.0": "Apache License 2.0",
        "apache 2.0": "Apache License 2.0",
        "apache 2": "Apache License 2.0",
        "apache license 2.0": "Apache License 2.0",
        "apache license v2": "Apache License 2.0",
        "apache license version 2": "Apache License 2.0",
        "apache license 2": "Apache License 2.0",
        "gpl-3.0": "GNU General Public License",
        "gpl 3": "GNU General Public License",
        "gplv3": "GNU General Public License",
        "gpl-2.0": "GNU General Public License",
        "gpl 2": "GNU General Public License",
        "gplv2": "GNU General Public License",
        "lgpl-3.0": "GNU Lesser General Public License",
        "lgpl 3": "GNU Lesser General Public License",
        "lgplv3": "GNU Lesser General Public License",
        "bsd-3-clause": "BSD 3 Clause License",
        "bsd 3 clause": "BSD 3 Clause License",
        "bsd 3-clause license": "BSD 3 Clause License",
        "bsd-2-clause": "BSD 2 Clause License",
        "bsd 2 clause": "BSD 2 Clause License",
        "bsd 2-clause license": "BSD 2 Clause License",
        "mpl-2.0": "Mozilla Public License 2.0",
        "mpl 2.0": "Mozilla Public License 2.0",
        "unlicense": "Unlicense",
    }

    # Language shorthands
    lang_map = {
        "js": "JavaScript",
        "ts": "TypeScript",
        "typescript": "TypeScript",
        "type script": "TypeScript",
        "type-script": "TypeScript",
        "py": "Python",
        "py3": "Python",
        "c#": "C Sharp",
        "c sharp": "C Sharp",
        "csharp": "C Sharp",
        "c++": "C++",
        "c plus plus": "C++",
        "cpp": "C++",
        "c/c++": "C++",
        "objc": "Objective-C",
        "objective-c": "Objective-C",
        "objective c": "Objective-C",
        "golang": "Go",
        "vue.js": "Vue.js",
        "vue js": "Vue.js",
        "nodejs": "Node.js",
        "node": "Node.js",
        "node js": "Node.js",
    }

    # For topics, they might be comma-separated
    if entity_type == 'TOPICS':
        # Split and normalize each topic
        topics = value.split(',')
        # Return the first topic for now (we could explode these later)
        if topics:
            value = topics[0].strip()

    if entity_type == 'URL':
        try:
            parsed = urlparse(value)
        except Exception:
            # If urlparse fails (e.g., malformed IPv6 URL), fall back to raw value
            return normalize_title(re.sub(r'[-_]+', ' ', value))
        segments = [seg for seg in parsed.path.split('/') if seg]
        # Prefer the last meaningful segment
        if segments:
            candidate = segments[-1]
            candidate = re.sub(r'\.(git|html|htm)$', '', candidate, flags=re.IGNORECASE)
        else:
            candidate = parsed.netloc or value
        candidate = unquote(candidate)
        # Replace delimiters with spaces before normalization
        candidate = re.sub(r'[-_]+', ' ', candidate)
        return normalize_title(candidate)

    if entity_type == 'LICENSE':
        lowered = value.strip().lower()
        normalized_lowered = re.sub(r'[_/]+', ' ', lowered)
        mapped = license_map.get(lowered, license_map.get(normalized_lowered, value))
        return normalize_title(mapped)

    if entity_type == 'LANG_STATS':
        lowered = value.strip().lower()
        normalized_lowered = re.sub(r'[\\/_-]+', ' ', lowered)
        mapped = lang_map.get(lowered, lang_map.get(normalized_lowered, value))
        return normalize_title(mapped)

    # Apply standard normalization (same as Wikipedia titles)
    return normalize_title(value)


def build_join_base(
    entities_df: DataFrame,
    canonical_df: DataFrame,
) -> (DataFrame, Dict[str, int]):
    """
    Join entities with Wikipedia canonical data but keep only lightweight columns.
    Heavy wiki enrichment (categories/abstracts) is added later to avoid
    materializing large strings during statistics.
    """
    normalize_udf = F.udf(normalize_entity_value, StringType())

    supported_types = get_supported_entity_types()
    entities_filtered = entities_df.filter(F.col("type").isin(supported_types))

    entities_normalized = entities_filtered.withColumn(
        "norm_value", normalize_udf(F.col("value"), F.col("type"))
    )

    entities_exploded = entities_normalized.withColumn(
        "value_array",
        F.when(F.col("type") == "TOPICS", F.split(F.col("value"), ",")).otherwise(F.array(F.col("value")))
    ).select(
        F.col("doc_id"),
        F.col("type").alias("entity_type"),
        F.explode(F.col("value_array")).alias("entity_value"),
        F.col("offsets_json")
    ).withColumn(
        "norm_value", normalize_udf(F.trim(F.col("entity_value")), F.col("entity_type"))
    ).withColumn(
        "norm_value_compact",
        F.regexp_replace(F.col("norm_value"), r"[\s_.-]+", "")
    )

    norm_stats = entities_exploded.agg(
        F.count("*").alias("total_entities"),
        F.sum(F.when(F.col("norm_value") == "", 1).otherwise(0)).alias("empty_norm_values")
    ).collect()[0]

    entities_exploded = entities_exploded.filter(F.col("norm_value") != "")

    join_condition = (
        (F.col("e.norm_value") == F.col("c.norm_key")) |
        (F.col("e.norm_value_compact") == F.col("c.norm_key_compact"))
    )
    joined_df = entities_exploded.alias("e").join(
        canonical_df.alias("c"),
        join_condition,
        "left"
    ).select(
        F.col("e.doc_id"),
        F.col("e.entity_type"),
        F.col("e.entity_value"),
        F.col("e.norm_value"),
        F.col("e.norm_value_compact"),
        F.col("c.page_id").alias("wiki_page_id"),
        F.col("c.wiki_title"),
        F.col("c.norm_key").alias("join_key"),
        F.col("c.match_type"),
        F.when(F.col("e.norm_value") == F.col("c.norm_key"), F.lit("exact"))
         .when(F.col("e.norm_value_compact") == F.col("c.norm_key_compact"), F.lit("compact"))
         .otherwise(F.lit(None)).alias("match_source")
    )

    match_rank = F.when(F.col("match_type") == "direct", F.lit(0)) \
        .when(F.col("match_type") == "redirect", F.lit(1)) \
        .when(F.col("match_type").isNotNull(), F.lit(2)) \
        .otherwise(F.lit(3))
    source_rank = F.when(F.col("match_source") == "exact", F.lit(0)) \
        .when(F.col("match_source") == "compact", F.lit(1)) \
        .otherwise(F.lit(2))

    dedup_window = Window.partitionBy(
        "doc_id", "entity_type", "entity_value", "norm_value", "norm_value_compact"
    ).orderBy(match_rank, source_rank, F.col("wiki_page_id"))

    joined_df = joined_df.withColumn("row_rank", F.row_number().over(dedup_window)) \
        .filter(F.col("row_rank") == 1) \
        .drop("row_rank", "match_source")

    norm_stats_dict = {
        "entities_seen": int(norm_stats["total_entities"]),
        "empty_norm_values": int(norm_stats["empty_norm_values"]),
        "entities_used": int(norm_stats["total_entities"] - norm_stats["empty_norm_values"]),
    }

    return joined_df, norm_stats_dict


def enrich_join_with_wiki_content(
    joined_df: DataFrame,
    categories_df: Optional[DataFrame],
    abstracts_df: Optional[DataFrame]
) -> DataFrame:
    """
    Add wiki categories, abstracts, and confidence labels.
    """
    if categories_df is not None:
        categories_agg = categories_df.groupBy("page_id").agg(
            F.concat_ws("|", F.collect_list("category")).alias("wiki_categories"),
            F.max(
                F.when(
                    F.col("norm_category").rlike("programming|language|license|software|technology|framework|library|computer"),
                    F.lit(True)
                ).otherwise(F.lit(False))
            ).alias("has_relevant_category")
        )

        joined_df = joined_df.join(
            categories_agg,
            joined_df.wiki_page_id == categories_agg.page_id,
            "left"
        ).drop(categories_agg.page_id)

        joined_df = joined_df.fillna({"has_relevant_category": False, "wiki_categories": ""})
    else:
        joined_df = joined_df.withColumn("has_relevant_category", F.lit(False))
        joined_df = joined_df.withColumn("wiki_categories", F.lit(""))

    if abstracts_df is not None:
        joined_df = joined_df.join(
            abstracts_df.select(
                F.col("page_id"),
                F.col("abstract_text").alias("wiki_abstract")
            ),
            joined_df.wiki_page_id == abstracts_df.page_id,
            "left"
        ).drop(abstracts_df.page_id)

        joined_df = joined_df.fillna({"wiki_abstract": ""})
    else:
        joined_df = joined_df.withColumn("wiki_abstract", F.lit(""))

    confidence_expr = F.when(
        F.col("wiki_page_id").isNull(),
        F.lit(None).cast(StringType())
    ).otherwise(
        F.when(
            F.col("match_type") == "direct",
            F.when(F.col("has_relevant_category"), F.lit("exact+cat"))
             .when(F.col("wiki_abstract") != "", F.lit("exact+abs"))
             .otherwise(F.lit("exact"))
        ).when(
            F.col("match_type") == "redirect",
            F.when(F.col("has_relevant_category"), F.lit("redirect+cat"))
             .when(F.col("wiki_abstract") != "", F.lit("redirect+abs"))
             .otherwise(F.lit("redirect"))
        ).otherwise(
            F.when(F.col("has_relevant_category"), F.lit("alias+cat"))
             .when(F.col("wiki_abstract") != "", F.lit("alias+abs"))
             .otherwise(F.lit("alias"))
        )
    )

    joined_df = joined_df.withColumn("confidence", confidence_expr)

    return joined_df.select(
        "doc_id",
        "entity_type",
        "entity_value",
        "norm_value",
        "wiki_page_id",
        "wiki_title",
        "wiki_abstract",
        "wiki_categories",
        "join_key",
        "confidence"
    )


def generate_statistics(joined_df: DataFrame, logger) -> Dict:
    """
    Generate join statistics using a SINGLE aggregation pass (approx distinct to reduce shuffle).

    STREAMING OPTIMIZATION: Uses a single groupBy aggregation instead of
    multiple count() operations. This avoids multiple full scans of the data.
    Reference: https://spark.apache.org/docs/latest/tuning.html
    """
    logger.info("Generating statistics (single aggregation pass)...")

    # Single aggregation to get all stats at once - avoids multiple scans
    # This is much more efficient than multiple count() calls
    overall_stats = joined_df.agg(
        F.count("*").alias("total_entities"),
        F.sum(F.when(F.col("wiki_page_id").isNotNull(), 1).otherwise(0)).alias("matched_entities"),
        F.approx_count_distinct(F.when(F.col("wiki_page_id").isNotNull(), F.col("wiki_page_id")), rsd=0.01).alias("unique_wiki_pages"),
        F.approx_count_distinct(F.when(F.col("wiki_page_id").isNotNull(), F.col("doc_id")), rsd=0.01).alias("unique_docs_with_wiki")
    ).collect()[0]  # Single row result - safe to collect

    total_entities = overall_stats["total_entities"]
    matched_entities = overall_stats["matched_entities"]
    unique_wiki_pages_joined = overall_stats["unique_wiki_pages"]
    unique_docs_with_wiki = overall_stats["unique_docs_with_wiki"]

    # Stats by entity type - single groupBy, small result set safe to collect
    type_stats = joined_df.groupBy("entity_type").agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("wiki_page_id").isNotNull(), 1).otherwise(0)).alias("matched"),
        F.approx_count_distinct(F.when(F.col("wiki_page_id").isNotNull(), F.col("wiki_page_id")), rsd=0.01).alias("unique_pages")
    ).collect()  # Small result set (few entity types) - safe to collect

    stats = {
        "total_entities": total_entities,
        "matched_entities": matched_entities,
        "match_rate": round(matched_entities / total_entities * 100, 2) if total_entities > 0 else 0,
        "unique_wiki_pages_joined": unique_wiki_pages_joined,
        "unique_docs_with_wiki": unique_docs_with_wiki,
        "by_type": {
            row["entity_type"]: {
                "total": row["total"],
                "matched": row["matched"],
                "unique_pages": row["unique_pages"],
                "rate": round(row["matched"] / row["total"] * 100, 2) if row["total"] > 0 else 0
            }
            for row in type_stats
        }
    }

    return stats


def main() -> int:
    """Main entry point."""
    args = _load_env_args()
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
    struct_logger.log("start", entities=args.entities, wiki=args.wiki, out=args.out)

    # Paths
    entities_path = Path(args.entities)
    wiki_dir = Path(args.wiki)
    output_dir = Path(args.out)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not entities_path.exists():
        logger.error(f"Entities file not found: {entities_path}")
        return 1

    if not wiki_dir.exists():
        logger.error(f"Wiki directory not found: {wiki_dir}")
        return 1

    # Create Spark session with DataFrame-optimized configuration
    spark = SparkSession.builder \
        .appName("WikiHtmlJoin-DataFrame") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '6g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '3g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load entities - NO count() to avoid full scan
        logger.info("Loading entities (lazy evaluation)...")
        entities_df = load_entities(spark, entities_path, args.entities_max_rows)
        # Don't call count() here - let it be computed during statistics generation
        logger.info(f"Loaded entities from {entities_path}")
        struct_logger.log("entities_loaded", path=str(entities_path))

        # Load Wikipedia data - NO count() calls (streaming optimization)
        logger.info("Loading Wikipedia data (lazy evaluation)...")
        wiki_data = load_wiki_data(spark, wiki_dir, logger)

        if 'pages' not in wiki_data:
            logger.error("Wikipedia pages not found")
            return 1

        # Prepare canonical mapping
        # Use DISK_ONLY persistence to avoid OOM on large datasets
        # Reference: https://spark.apache.org/docs/latest/tuning.html
        logger.info("Building canonical mapping (DISK_ONLY persistence)...")
        canonical_df = prepare_canonical_mapping(
            wiki_data['pages'],
            wiki_data.get('aliases')
        )
        # Use DISK_ONLY to spill to disk if memory is full - OOM safe
        canonical_df = canonical_df.persist(StorageLevel.DISK_ONLY)
        logger.info("Canonical mapping prepared (will materialize during join)")

        # Perform join with categories and abstracts
        logger.info("Joining entities with Wikipedia...")
        join_base_df, norm_stats = build_join_base(
            entities_df,
            canonical_df
        )
        logger.info(f"Entity normalization stats: {norm_stats}")
        struct_logger.log("normalization", **norm_stats)

        # Persist base join (without heavy wiki strings) for stats + downstream enrichment
        logger.info("Persisting base joined DataFrame with DISK_ONLY storage (lightweight columns only)...")
        join_base_df = join_base_df.persist(StorageLevel.DISK_ONLY)
        logger.info("Base DataFrame marked for persistence (will materialize during stats generation)")

        if args.stats_enabled:
            logger.info("Generating join statistics (this will materialize the persisted DataFrame)...")
            stats = generate_statistics(join_base_df, logger)
            logger.info("DataFrame successfully materialized to disk during stats generation")
            logger.info(f"Join statistics: {json.dumps(stats, indent=2)}")
            struct_logger.log("join_stats", **stats)
            entity_count = stats['total_entities']
        else:
            logger.info("JOIN_STATS disabled; skipping statistics aggregation to save time/resources")
            stats = {
                "stats_disabled": True,
                "reason": "JOIN_STATS=false",
                "total_entities_estimate": norm_stats["entities_used"]
            }
            struct_logger.log("join_stats_skipped", **stats)
            entity_count = norm_stats["entities_used"]

        # Pre-write validation: Check if we have data to write
        if entity_count == 0:
            logger.warning("No entities found after join - output files will be empty or minimal")

        if not args.dry_run:
            # Ensure output directory exists and is writable
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
                # Test write permission
                test_file = output_dir / ".write_test"
                test_file.write_text("test")
                test_file.unlink()
                logger.info(f"✓ Output directory verified: {output_dir}")
            except Exception as e:
                logger.error(f"✗ Output directory not writable: {output_dir}")
                logger.error(f"Error: {e}")
                raise PermissionError(f"Cannot write to output directory: {output_dir}") from e

            logger.info("=" * 60)
            logger.info("STARTING FILE WRITES")
            logger.info(f"Total entities to write: {entity_count:,}")
            logger.info(f"Output directory: {output_dir}")
            logger.info(f"Write mode: {'COALESCE(1)' if args.coalesce_output else 'MERGE_PARTS'}")
            logger.info("=" * 60)
            # Write main join results (includes wiki_abstract and wiki_categories)
            try:
                logger.info("Enriching join with categories/abstracts for output...")
                joined_df = enrich_join_with_wiki_content(
                    join_base_df,
                    wiki_data.get('categories'),
                    wiki_data.get('abstracts')
                )

                logger.info("Writing main join results (html_wiki.tsv)...")
                output_path = output_dir / "html_wiki.tsv"
                write_tsv(
                    joined_df,
                    output_path,
                    ["doc_id", "entity_type", "entity_value", "norm_value",
                     "wiki_page_id", "wiki_title", "wiki_abstract", "wiki_categories",
                     "join_key", "confidence"],
                    header=True,
                    single_file=args.coalesce_output,
                    max_records_per_file=args.max_records_per_file,
                    merge_after_write=not args.coalesce_output  # parallel write then merge to single TSV
                )
                # Validate file was created
                if output_path.exists():
                    file_size = output_path.stat().st_size
                    logger.info(f"✓ Wrote join results to {output_path} ({file_size:,} bytes)")
                else:
                    logger.error(f"✗ FAILED: Output file not created: {output_path}")
                    raise FileNotFoundError(f"Expected output file not found: {output_path}")
            except Exception as e:
                logger.error(f"✗ FAILED to write main join results: {e}")
                logger.exception("Full traceback:")
                raise

            # Write aggregated statistics (optional)
            if args.stats_enabled:
                try:
                    logger.info("Writing aggregated statistics (join_stats.json)...")
                    stats_path = output_dir / "join_stats.json"
                    stats_path.write_text(json.dumps(stats, indent=2))
                    if stats_path.exists():
                        logger.info(f"✓ Wrote statistics to {stats_path} ({stats_path.stat().st_size:,} bytes)")
                    else:
                        logger.error(f"✗ FAILED: Stats file not created: {stats_path}")
                except Exception as e:
                    logger.error(f"✗ FAILED to write statistics: {e}")
                    logger.exception("Full traceback:")
                    raise

            # Write per-document aggregates with proper fields per spec
            try:
                logger.info("Writing per-document aggregates (html_wiki_agg.tsv)...")
                doc_agg = join_base_df.filter(F.col("wiki_page_id").isNotNull()).groupBy("doc_id").agg(
                    F.concat_ws(",", F.collect_set(F.col("wiki_page_id").cast("string"))).alias("joined_page_ids"),
                    F.countDistinct("wiki_page_id").alias("num_joined_pages"),
                    F.sum(F.when(F.col("entity_type") == "TOPICS", 1).otherwise(0)).alias("num_topics_joined"),
                    F.sum(F.when(F.col("entity_type") == "LICENSE", 1).otherwise(0)).alias("num_licenses_joined"),
                    F.sum(F.when(F.col("entity_type") == "LANG_STATS", 1).otherwise(0)).alias("num_langs_joined")
                )
                agg_path = output_dir / "html_wiki_agg.tsv"
                write_tsv(
                    doc_agg, agg_path,
                    ["doc_id", "joined_page_ids", "num_joined_pages",
                     "num_topics_joined", "num_licenses_joined", "num_langs_joined"],
                    header=True,
                    single_file=args.coalesce_output,
                    max_records_per_file=args.max_records_per_file,
                    merge_after_write=not args.coalesce_output
                )
                # Validate file was created
                if agg_path.exists():
                    file_size = agg_path.stat().st_size
                    logger.info(f"✓ Wrote document aggregates to {agg_path} ({file_size:,} bytes)")
                else:
                    logger.error(f"✗ FAILED: Aggregates file not created: {agg_path}")
                    raise FileNotFoundError(f"Expected aggregates file not found: {agg_path}")
            except Exception as e:
                logger.error(f"✗ FAILED to write document aggregates: {e}")
                logger.exception("Full traceback:")
                raise

        # Write manifest
        duration = time.time() - start_time
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": round(duration, 2),
            "api": "DataFrame",
            "inputs": {
                "entities": str(entities_path),
                "wiki_dir": str(wiki_dir),
                "entity_count": entity_count,
                "entities_max_rows": args.entities_max_rows,
            },
            "outputs": {
                "join": str(output_dir / "html_wiki.tsv"),
                "stats": str(output_dir / "join_stats.json") if args.stats_enabled else None,
                "aggregates": str(output_dir / "html_wiki_agg.tsv"),
            },
            "statistics": stats,
            "spark_config": {
                "driver_memory": os.environ.get('SPARK_DRIVER_MEMORY', '6g'),
                "partitions": args.partitions,
            },
            "features": {
                "single_file_output": args.coalesce_output,
                "max_records_per_file": args.max_records_per_file
            }
        }

        runs_dir = Path('runs') / datetime.now().strftime('%Y%m%d_%H%M%S')
        runs_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = runs_dir / 'manifest.json'
        write_manifest(manifest_path, manifest)

        # Save pipeline stats
        pipeline_stats = PipelineStats("join")
        pipeline_stats.set_config(
            partitions=args.partitions,
            entities_max_rows=args.entities_max_rows,
            single_file_output=args.coalesce_output,
            max_records_per_file=args.max_records_per_file
        )
        pipeline_stats.set_inputs(
            entities_file=str(entities_path),
            wiki_dir=str(wiki_dir),
            entity_count=entity_count,
            total_items=entity_count
        )
        pipeline_stats.set_outputs(
            join_file=str(output_dir / "html_wiki.tsv"),
            stats_file=str(output_dir / "join_stats.json") if args.stats_enabled else None,
            aggregates_file=str(output_dir / "html_wiki_agg.tsv")
        )
        pipeline_stats.set_nested("join_statistics", "stats_enabled", args.stats_enabled)
        if args.stats_enabled:
            pipeline_stats.set_nested("join_statistics", "total_entities", stats['total_entities'])
            pipeline_stats.set_nested("join_statistics", "matched_entities", stats['matched_entities'])
            pipeline_stats.set_nested("join_statistics", "match_rate", stats['match_rate'])
            pipeline_stats.set_nested("join_statistics", "unique_wiki_pages_joined", stats['unique_wiki_pages_joined'])
            pipeline_stats.set_nested("join_statistics", "unique_docs_with_wiki", stats['unique_docs_with_wiki'])

            # Add entity type breakdown
            for entity_type, type_stat in stats['by_type'].items():
                pipeline_stats.set_entities(entity_type, type_stat['total'],
                                           matched=type_stat['matched'],
                                           unique_pages=type_stat['unique_pages'],
                                           match_rate=type_stat['rate'])
        else:
            pipeline_stats.set_nested("join_statistics", "total_entities_estimate", entity_count)

        entities_per_sec = (
            round(stats['total_entities'] / duration, 2)
            if args.stats_enabled and duration > 0
            else round(entity_count / duration, 2) if duration > 0 else 0
        )
        pipeline_stats.set_nested("performance", "entities_per_second", entities_per_sec)
        pipeline_stats.finalize("completed")
        stats_path_saved = pipeline_stats.save()
        logger.info(f"Stats saved to: {stats_path_saved}")

        # Generate pipeline summary
        try:
            save_pipeline_summary()
        except Exception as e:
            logger.warning(f"Failed to generate pipeline summary: {e}")

        # Log completion
        struct_logger.log("complete", duration=duration, stats=stats)

        # Clean up persisted DataFrames to free disk space
        logger.info("Cleaning up persisted data...")
        try:
            if 'canonical_df' in locals():
                canonical_df.unpersist()
            if 'join_base_df' in locals():
                join_base_df.unpersist()
        except Exception as cleanup_err:
            logger.warning(f"Cleanup warning (non-fatal): {cleanup_err}")

        # Validate and report output files
        logger.info("=" * 60)
        logger.info("OUTPUT FILE VALIDATION")
        logger.info("=" * 60)
        output_files_ok = True
        if not args.dry_run:
            expected_files = [
                (output_dir / "html_wiki.tsv", "Main join results"),
                (output_dir / "html_wiki_agg.tsv", "Document aggregates"),
            ]
            if args.stats_enabled:
                expected_files.append((output_dir / "join_stats.json", "Join statistics"))
            for file_path, description in expected_files:
                if file_path.exists():
                    size_mb = file_path.stat().st_size / (1024 * 1024)
                    logger.info(f"✓ {description}: {file_path} ({size_mb:.2f} MB)")
                else:
                    logger.error(f"✗ MISSING: {description}: {file_path}")
                    output_files_ok = False

            if not output_files_ok:
                logger.error("Some output files are missing - job may have failed")
        else:
            logger.info("Dry-run mode - no files written")

        # Print summary
        logger.info("=" * 60)
        logger.info("WIKI-HTML JOIN COMPLETE (DataFrame API - Streaming Optimized)")
        logger.info(f"Duration: {duration:.2f} seconds")
        if args.stats_enabled:
            logger.info(f"Total entities: {stats['total_entities']:,}")
            logger.info(f"Matched entities: {stats['matched_entities']:,} ({stats['match_rate']}%)")
            logger.info(f"Unique Wikipedia pages joined: {stats['unique_wiki_pages_joined']:,}")
            logger.info(f"Unique docs with wiki matches: {stats['unique_docs_with_wiki']:,}")
            logger.info("Match rates by type:")
            for entity_type, type_stat in stats['by_type'].items():
                logger.info(f"  {entity_type}: {type_stat['matched']:,}/{type_stat['total']:,} ({type_stat['rate']}%) - {type_stat['unique_pages']:,} unique pages")
        else:
            logger.info(f"Statistics disabled; total entities estimate (after normalization): {entity_count:,}")
        logger.info(f"Manifest: {manifest_path}")
        if not args.dry_run and output_files_ok:
            logger.info("✓ All output files validated successfully")
        logger.info("=" * 60)

    finally:
        spark.stop()
        struct_logger.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
