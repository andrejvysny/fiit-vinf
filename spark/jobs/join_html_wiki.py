#!/usr/bin/env python3
"""
Spark job for joining HTML entities with Wikipedia canonical data.

Joins entities extracted from HTML (GitHub pages) with Wikipedia pages based on
normalized entity values. Handles aliases, calculates confidence scores, and
resolves collisions.

Supported entity types:
- LANG/LANG_STATS: Programming languages
- LICENSE: Software licenses
- TOPICS: Repository topics
- README: Keywords from README files
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Add project root to path
sys.path.insert(0, '/opt/app')

from spark.lib.wiki_regexes import normalize_title
from spark.lib.io import read_tsv, write_tsv, write_ndjson
from spark.lib.utils import StructuredLogger, write_manifest


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Join HTML entities with Wikipedia canonical data"
    )
    parser.add_argument(
        '--entities',
        required=True,
        help='Path to entities.tsv from HTML extraction'
    )
    parser.add_argument(
        '--wiki',
        required=True,
        help='Directory with Wikipedia TSV files'
    )
    parser.add_argument(
        '--out',
        default='workspace/store/join',
        help='Output directory for join results'
    )
    parser.add_argument(
        '--entities-max-rows',
        type=int,
        help='Maximum number of entity rows to process (for development)'
    )
    parser.add_argument(
        '--partitions',
        type=int,
        default=64,
        help='Number of Spark partitions'
    )
    parser.add_argument(
        '--log',
        default='logs/wiki_join.jsonl',
        help='Log file path'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview without writing outputs'
    )
    return parser.parse_args()


def load_entities(spark: SparkSession, path: Path, max_rows: Optional[int] = None) -> DataFrame:
    """Load entities TSV with optional row limit."""
    entities_df = read_tsv(spark, path, header=True)

    # Apply row limit if specified
    if max_rows:
        entities_df = entities_df.limit(max_rows)

    return entities_df


def load_wiki_data(spark: SparkSession, wiki_dir: Path) -> Dict[str, DataFrame]:
    """Load Wikipedia TSV files."""
    wiki_data = {}

    # Load pages
    pages_path = wiki_dir / "pages.tsv"
    if pages_path.exists():
        wiki_data['pages'] = read_tsv(spark, pages_path, header=True)
        logging.info(f"Loaded {wiki_data['pages'].count()} pages")

    # Load aliases
    aliases_path = wiki_dir / "aliases.tsv"
    if aliases_path.exists():
        wiki_data['aliases'] = read_tsv(spark, aliases_path, header=True)
        logging.info(f"Loaded {wiki_data['aliases'].count()} aliases")

    # Load categories
    categories_path = wiki_dir / "categories.tsv"
    if categories_path.exists():
        wiki_data['categories'] = read_tsv(spark, categories_path, header=True)
        logging.info(f"Loaded {wiki_data['categories'].count()} categories")

    # Load infobox (optional, for additional context)
    infobox_path = wiki_dir / "infobox.tsv"
    if infobox_path.exists():
        wiki_data['infobox'] = read_tsv(spark, infobox_path, header=True)

    return wiki_data


def prepare_canonical_mapping(pages_df: DataFrame, aliases_df: Optional[DataFrame]) -> DataFrame:
    """
    Create canonical mapping from normalized titles to page info.
    Includes both direct pages and aliases.
    """
    # Create canonical entries from pages
    canonical_df = pages_df.filter(
        (F.col("ns") == 0) & F.col("redirect_to").isNull()
    ).select(
        F.col("norm_title").alias("norm_key"),
        F.col("page_id"),
        F.col("title").alias("wiki_title"),
        F.lit("direct").alias("match_type")
    )

    # If we have aliases, join them to get canonical pages
    if aliases_df is not None:
        # Join aliases with pages to get canonical page info
        alias_canonical = aliases_df.alias("a").join(
            pages_df.alias("p"),
            F.col("a.canonical_norm_title") == F.col("p.norm_title"),
            "inner"
        ).select(
            F.col("a.alias_norm_title").alias("norm_key"),
            F.col("p.page_id"),
            F.col("p.title").alias("wiki_title"),
            F.lit("alias").alias("match_type")
        )

        # Union direct and alias mappings
        canonical_df = canonical_df.union(alias_canonical)

    # Deduplicate (prefer direct matches over aliases)
    window = Window.partitionBy("norm_key").orderBy(
        F.when(F.col("match_type") == "direct", 0).otherwise(1),
        F.col("page_id")
    )
    canonical_df = canonical_df.withColumn(
        "rank", F.row_number().over(window)
    ).filter(F.col("rank") == 1).drop("rank")

    return canonical_df


def get_supported_entity_types() -> List[str]:
    """Return list of entity types we support for joining."""
    return [
        'LANG_STATS',     # Programming language statistics
        'LICENSE',        # Software licenses
        'TOPICS',         # GitHub topics
        'README',         # Keywords from README
        'STAR_COUNT',     # For filtering (not joining)
        'FORK_COUNT',     # For filtering (not joining)
        'URL',            # Could be used for external links
    ]


def normalize_entity_value(value: str, entity_type: str) -> str:
    """
    Normalize entity value for matching with Wikipedia titles.
    Different normalization strategies per entity type.
    """
    if not value:
        return ""

    # For topics, they might be comma-separated
    if entity_type == 'TOPICS':
        # Split and normalize each topic
        topics = value.split(',')
        # Return the first topic for now (we could explode these later)
        if topics:
            value = topics[0].strip()

    # Apply standard normalization (same as Wikipedia titles)
    return normalize_title(value)


def calculate_confidence(row) -> float:
    """
    Calculate confidence score for a match.
    Factors:
    - Direct match vs alias
    - Category hints
    - Case sensitivity
    """
    confidence = 0.6  # Base confidence

    # Match type bonus
    if row['match_type'] == 'direct':
        confidence += 0.2
    elif row['match_type'] == 'alias':
        confidence += 0.1

    # Exact case match bonus
    if row.get('entity_value', '').lower() == row.get('wiki_title', '').lower():
        confidence += 0.1

    # Category hint bonus (would need categories joined)
    if row.get('has_relevant_category'):
        confidence += 0.1

    return min(confidence, 1.0)


def join_entities_with_wiki(
    entities_df: DataFrame,
    canonical_df: DataFrame,
    categories_df: Optional[DataFrame] = None
) -> DataFrame:
    """
    Join entities with Wikipedia canonical data.
    """
    # Register UDF for normalization
    normalize_udf = F.udf(normalize_entity_value, StringType())

    # Filter to supported entity types
    supported_types = get_supported_entity_types()
    entities_filtered = entities_df.filter(
        F.col("type").isin(supported_types)
    )

    # Normalize entity values
    entities_normalized = entities_filtered.withColumn(
        "norm_value", normalize_udf(F.col("value"), F.col("type"))
    )

    # For TOPICS, we should explode them
    # If type is TOPICS and value contains commas, explode
    entities_exploded = entities_normalized.withColumn(
        "value_array",
        F.when(
            F.col("type") == "TOPICS",
            F.split(F.col("value"), ",")
        ).otherwise(F.array(F.col("value")))
    ).select(
        F.col("doc_id"),
        F.col("type").alias("entity_type"),
        F.explode(F.col("value_array")).alias("entity_value"),
        F.col("offsets_json")
    ).withColumn(
        "norm_value", normalize_udf(F.trim(F.col("entity_value")), F.col("entity_type"))
    )

    # Join with canonical mapping
    joined_df = entities_exploded.alias("e").join(
        canonical_df.alias("c"),
        F.col("e.norm_value") == F.col("c.norm_key"),
        "left"
    ).select(
        F.col("e.doc_id"),
        F.col("e.entity_type"),
        F.col("e.entity_value"),
        F.col("e.norm_value"),
        F.col("c.page_id").alias("wiki_page_id"),
        F.col("c.wiki_title"),
        F.col("c.norm_key").alias("join_key"),
        F.col("c.match_type")
    )

    # Add category signals if available
    if categories_df is not None:
        # Check for relevant categories (programming languages, licenses, etc.)
        relevant_categories = categories_df.filter(
            F.col("norm_category").rlike("programming|language|license|software|technology")
        ).select("page_id").distinct().withColumn("has_relevant_category", F.lit(True))

        joined_df = joined_df.join(
            relevant_categories,
            joined_df.wiki_page_id == relevant_categories.page_id,
            "left"
        ).fillna({"has_relevant_category": False})
    else:
        joined_df = joined_df.withColumn("has_relevant_category", F.lit(False))

    # Calculate confidence scores
    confidence_udf = F.udf(calculate_confidence, FloatType())
    joined_df = joined_df.withColumn(
        "confidence",
        F.when(F.col("wiki_page_id").isNotNull(),
               F.round(confidence_udf(F.struct(joined_df.columns)), 2)
        ).otherwise(None)
    )

    # Select final columns
    final_df = joined_df.select(
        "doc_id",
        "entity_type",
        "entity_value",
        "norm_value",
        "wiki_page_id",
        "wiki_title",
        "join_key",
        "confidence"
    )

    return final_df


def generate_statistics(joined_df: DataFrame) -> Dict:
    """Generate join statistics."""
    total_entities = joined_df.count()
    matched_entities = joined_df.filter(F.col("wiki_page_id").isNotNull()).count()
    unique_wiki_pages = joined_df.filter(F.col("wiki_page_id").isNotNull()).select("wiki_page_id").distinct().count()

    # Stats by entity type
    type_stats = joined_df.groupBy("entity_type").agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("wiki_page_id").isNotNull(), 1).otherwise(0)).alias("matched")
    ).collect()

    stats = {
        "total_entities": total_entities,
        "matched_entities": matched_entities,
        "match_rate": round(matched_entities / total_entities * 100, 2) if total_entities > 0 else 0,
        "unique_wiki_pages": unique_wiki_pages,
        "by_type": {
            row["entity_type"]: {
                "total": row["total"],
                "matched": row["matched"],
                "rate": round(row["matched"] / row["total"] * 100, 2) if row["total"] > 0 else 0
            }
            for row in type_stats
        }
    }

    return stats


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

    # Create Spark session
    spark = SparkSession.builder \
        .appName("WikiHtmlJoin") \
        .master("local[*]") \
        .config("spark.driver.memory", os.environ.get('SPARK_DRIVER_MEMORY', '6g')) \
        .config("spark.executor.memory", os.environ.get('SPARK_EXECUTOR_MEMORY', '3g')) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", str(args.partitions)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load entities
        logger.info("Loading entities...")
        entities_df = load_entities(spark, entities_path, args.entities_max_rows)
        entity_count = entities_df.count()
        logger.info(f"Loaded {entity_count} entities")
        struct_logger.log("entities_loaded", count=entity_count)

        # Load Wikipedia data
        logger.info("Loading Wikipedia data...")
        wiki_data = load_wiki_data(spark, wiki_dir)

        if 'pages' not in wiki_data:
            logger.error("Wikipedia pages not found")
            return 1

        # Prepare canonical mapping
        logger.info("Building canonical mapping...")
        canonical_df = prepare_canonical_mapping(
            wiki_data['pages'],
            wiki_data.get('aliases')
        )
        canonical_df = canonical_df.cache()
        canonical_count = canonical_df.count()
        logger.info(f"Built canonical mapping with {canonical_count} entries")

        # Perform join
        logger.info("Joining entities with Wikipedia...")
        joined_df = join_entities_with_wiki(
            entities_df,
            canonical_df,
            wiki_data.get('categories')
        )

        # Generate statistics
        stats = generate_statistics(joined_df)
        logger.info(f"Join statistics: {json.dumps(stats, indent=2)}")
        struct_logger.log("join_stats", **stats)

        if not args.dry_run:
            # Write main join results
            output_path = output_dir / "html_wiki.tsv"
            write_tsv(
                joined_df,
                output_path,
                ["doc_id", "entity_type", "entity_value", "norm_value",
                 "wiki_page_id", "wiki_title", "join_key", "confidence"],
                header=True
            )
            logger.info(f"Wrote join results to {output_path}")

            # Write aggregated statistics
            stats_path = output_dir / "join_stats.json"
            stats_path.write_text(json.dumps(stats, indent=2))
            logger.info(f"Wrote statistics to {stats_path}")

            # Write per-document aggregates
            doc_agg = joined_df.groupBy("doc_id", "entity_type").agg(
                F.count("*").alias("total"),
                F.sum(F.when(F.col("wiki_page_id").isNotNull(), 1).otherwise(0)).alias("matched")
            )
            agg_path = output_dir / "html_wiki_agg.tsv"
            write_tsv(doc_agg, agg_path, ["doc_id", "entity_type", "total", "matched"], header=True)
            logger.info(f"Wrote document aggregates to {agg_path}")

        # Write manifest
        duration = time.time() - start_time
        manifest = {
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": round(duration, 2),
            "inputs": {
                "entities": str(entities_path),
                "wiki_dir": str(wiki_dir),
                "entity_count": entity_count,
                "entities_max_rows": args.entities_max_rows,
            },
            "outputs": {
                "join": str(output_dir / "html_wiki.tsv"),
                "stats": str(output_dir / "join_stats.json"),
                "aggregates": str(output_dir / "html_wiki_agg.tsv"),
            },
            "statistics": stats,
            "spark_config": {
                "driver_memory": os.environ.get('SPARK_DRIVER_MEMORY', '6g'),
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
        logger.info("WIKI-HTML JOIN COMPLETE")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Total entities: {stats['total_entities']}")
        logger.info(f"Matched entities: {stats['matched_entities']} ({stats['match_rate']}%)")
        logger.info(f"Unique Wikipedia pages: {stats['unique_wiki_pages']}")
        logger.info("Match rates by type:")
        for entity_type, type_stat in stats['by_type'].items():
            logger.info(f"  {entity_type}: {type_stat['matched']}/{type_stat['total']} ({type_stat['rate']}%)")
        logger.info(f"Manifest: {manifest_path}")
        logger.info("=" * 60)

    finally:
        spark.stop()
        struct_logger.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())