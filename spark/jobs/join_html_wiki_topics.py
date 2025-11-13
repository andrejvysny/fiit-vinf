#!/usr/bin/env python3
"""
PySpark Structured Streaming JOIN: GitHub HTML entities → Wikipedia topics

Joins GitHub repository TOPICS with relevant Wikipedia articles using:
- Alias resolution (redirect handling)
- Category-based relevance filtering
- Abstract text matching

Outputs:
- html_wiki_topics.tsv (streaming append)
- html_wiki_topics_stats.tsv (per-batch stats via foreachBatch)
"""

import argparse
import json
import logging
import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def normalize_text(text: str) -> str:
    """
    Normalize text for matching:
    - Lowercase
    - ASCII-fold (remove accents)
    - Collapse spaces and punctuation
    - Strip parentheticals
    """
    if not text:
        return ""

    # Remove parenthetical suffixes
    text = re.sub(r'\s*\([^)]*\)\s*$', '', text)

    # Lowercase
    text = text.lower()

    # ASCII-fold (remove diacritics)
    text = ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    )

    # Replace punctuation and collapse spaces
    text = re.sub(r'[^a-z0-9\s]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def is_relevant_category(category: str, keywords: List[str]) -> bool:
    """Check if category contains any relevance keywords."""
    if not category:
        return False

    category_lower = category.lower()
    return any(kw in category_lower for kw in keywords)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Stream JOIN GitHub HTML entities with Wikipedia topics'
    )

    parser.add_argument(
        '--entities',
        required=True,
        help='Path to HTML entities TSV (streaming source)'
    )
    parser.add_argument(
        '--wiki',
        required=True,
        help='Directory containing Wiki TSV files (batch sources)'
    )
    parser.add_argument(
        '--out',
        required=True,
        help='Output directory for joined results'
    )
    parser.add_argument(
        '--checkpoint',
        required=True,
        help='Checkpoint directory for streaming state'
    )
    parser.add_argument(
        '--maxFilesPerTrigger',
        type=int,
        default=64,
        help='Max files per trigger for streaming (default: 64)'
    )
    parser.add_argument(
        '--relevantCategories',
        default='programming,software,computer,library,framework,license',
        help='Comma-separated relevance keywords for category filtering'
    )
    parser.add_argument(
        '--absHit',
        type=bool,
        default=True,
        help='Enable abstract contains match (default: True)'
    )

    return parser.parse_args()


def load_wiki_dimensions(spark: SparkSession, wiki_dir: Path, relevant_keywords: List[str]) -> tuple:
    """
    Load Wiki dimension tables as batch DataFrames.

    Returns: (pages_df, aliases_df, categories_df, abstracts_df)
    """
    logger.info(f"Loading Wiki dimensions from {wiki_dir}")

    # Load pages (filter ns == 0 for main namespace only)
    pages_df = spark.read \
        .option("sep", "\t") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(wiki_dir / "pages.tsv")) \
        .filter(F.col("ns") == 0) \
        .select("page_id", "title", "norm_title")

    pages_count = pages_df.count()
    logger.info(f"Loaded {pages_count} pages (ns=0)")

    # Load aliases
    aliases_df = spark.read \
        .option("sep", "\t") \
        .option("header", "true") \
        .csv(str(wiki_dir / "aliases.tsv")) \
        .select("alias_norm_title", "canonical_norm_title")

    aliases_count = aliases_df.count()
    logger.info(f"Loaded {aliases_count} aliases")

    # Load categories
    categories_df = spark.read \
        .option("sep", "\t") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(wiki_dir / "categories.tsv")) \
        .select("page_id", "category", "norm_category")

    # Mark relevant categories
    def check_relevant(cat):
        return "true" if is_relevant_category(cat, relevant_keywords) else "false"

    relevant_cat_udf = F.udf(check_relevant, StringType())

    categories_df = categories_df.withColumn(
        "is_relevant",
        relevant_cat_udf(F.col("norm_category"))
    )

    categories_count = categories_df.count()
    logger.info(f"Loaded {categories_count} categories")

    # Load abstracts
    try:
        abstracts_df = spark.read \
            .option("sep", "\t") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(wiki_dir / "abstract.tsv")) \
            .select("page_id", "abstract_text")

        abstracts_count = abstracts_df.count()
        logger.info(f"Loaded {abstracts_count} abstracts")
    except Exception as e:
        logger.warning(f"Could not load abstracts: {e}")
        abstracts_df = spark.createDataFrame([], "page_id: long, abstract_text: string")

    return pages_df, aliases_df, categories_df, abstracts_df


def setup_streaming_source(spark: SparkSession, entities_path: str, max_files: int) -> DataFrame:
    """
    Set up streaming source for HTML entities TSV.
    Filter for TOPICS type and explode comma-separated values.
    """
    logger.info(f"Setting up streaming source from {entities_path}")

    # Register normalization UDF
    normalize_udf = F.udf(normalize_text, StringType())

    # Stream entities
    entities_stream = spark.readStream \
        .option("sep", "\t") \
        .option("header", "true") \
        .option("maxFilesPerTrigger", max_files) \
        .option("multiLine", "false") \
        .csv(entities_path) \
        .filter(F.col("type") == "TOPICS") \
        .select(
            F.col("doc_id"),
            F.col("value")
        )

    # Split comma-separated topics into array
    entities_stream = entities_stream.withColumn(
        "topics_array",
        F.split(F.col("value"), ",")
    )

    # Explode topics array to individual rows
    entities_stream = entities_stream.select(
        F.col("doc_id"),
        F.explode(F.col("topics_array")).alias("topic_value")
    )

    # Trim and normalize
    entities_stream = entities_stream.withColumn(
        "topic_value",
        F.trim(F.col("topic_value"))
    ).withColumn(
        "norm_value",
        normalize_udf(F.col("topic_value"))
    ).filter(
        F.length(F.col("norm_value")) > 0
    )

    return entities_stream


def perform_join(
    entities_stream: DataFrame,
    pages_df: DataFrame,
    aliases_df: DataFrame,
    categories_df: DataFrame,
    abstracts_df: DataFrame,
    relevant_keywords: List[str],
    enable_abs_hit: bool
) -> DataFrame:
    """
    Perform multi-hop join:
    1. Entities → Aliases (optional redirect resolution)
    2. Canonical title → Pages
    3. Pages → Categories + Abstracts
    4. Filter for relevance
    """
    logger.info("Performing streaming join...")

    # Step 1: Resolve aliases (left join)
    entities_with_canonical = entities_stream.join(
        aliases_df,
        entities_stream.norm_value == aliases_df.alias_norm_title,
        "left"
    ).select(
        entities_stream.doc_id,
        entities_stream.topic_value,
        entities_stream.norm_value,
        F.coalesce(
            aliases_df.canonical_norm_title,
            entities_stream.norm_value
        ).alias("canonical_title"),
        F.when(
            aliases_df.canonical_norm_title.isNotNull(), F.lit("alias")
        ).otherwise(F.lit("exact")).alias("join_method")
    )

    # Step 2: Join to pages on canonical title
    joined_pages = entities_with_canonical.join(
        pages_df,
        entities_with_canonical.canonical_title == pages_df.norm_title,
        "inner"
    ).select(
        entities_with_canonical.doc_id,
        entities_with_canonical.topic_value,
        entities_with_canonical.norm_value,
        entities_with_canonical.join_method,
        pages_df.page_id,
        pages_df.title.alias("wiki_title")
    )

    # Step 3: Aggregate categories per page
    categories_agg = categories_df.groupBy("page_id").agg(
        F.collect_list("category").alias("categories_list"),
        F.max(F.when(F.col("is_relevant") == "true", F.lit(True)).otherwise(F.lit(False))).alias("has_relevant_cat")
    )

    # Step 4: Join categories
    joined_with_cats = joined_pages.join(
        categories_agg,
        "page_id",
        "left"
    )

    # Step 5: Join abstracts
    joined_with_abs = joined_with_cats.join(
        abstracts_df,
        "page_id",
        "left"
    )

    # Step 6: Apply relevance filter
    if enable_abs_hit:
        # Match if category hit OR abstract contains normalized entity
        normalize_udf = F.udf(normalize_text, StringType())

        joined_filtered = joined_with_abs.withColumn(
            "abs_norm",
            normalize_udf(F.col("abstract_text"))
        ).filter(
            (F.col("has_relevant_cat") == True) |
            (F.col("abs_norm").contains(F.col("norm_value")))
        )
    else:
        # Match only on category
        joined_filtered = joined_with_abs.filter(
            F.col("has_relevant_cat") == True
        )

    # Step 7: Build confidence flag
    joined_filtered = joined_filtered.withColumn(
        "confidence",
        F.when(
            (F.col("join_method") == "exact") & (F.col("has_relevant_cat") == True),
            F.lit("exact+cat")
        ).when(
            (F.col("join_method") == "alias") & (F.col("has_relevant_cat") == True),
            F.lit("alias+cat")
        ).when(
            (F.col("join_method") == "exact"),
            F.lit("exact+abs")
        ).otherwise(F.lit("alias+abs"))
    )

    # Step 8: Format output
    result_df = joined_filtered.select(
        F.col("doc_id"),
        F.lit("TOPIC").alias("entity_type"),
        F.col("topic_value").alias("entity_value"),
        F.col("norm_value"),
        F.col("page_id").alias("wiki_page_id"),
        F.col("wiki_title"),
        F.col("join_method"),
        F.col("confidence"),
        F.to_json(F.col("categories_list")).alias("categories_json"),
        F.substring(F.col("abstract_text"), 1, 500).alias("abstract_text")
    )

    return result_df


def write_batch_stats(batch_df: DataFrame, batch_id: int, stats_path: Path):
    """
    Compute and write per-batch statistics.
    Called via foreachBatch on the streaming query.
    """
    try:
        # Compute stats
        rows_written = batch_df.count()

        if rows_written == 0:
            logger.info(f"Batch {batch_id}: No rows")
            return

        distinct_pages = batch_df.select("wiki_page_id").distinct().count()

        join_method_counts = batch_df.groupBy("join_method").count().collect()
        exact_count = sum(row['count'] for row in join_method_counts if row['join_method'] == 'exact')
        alias_count = sum(row['count'] for row in join_method_counts if row['join_method'] == 'alias')

        # Append stats to file
        stats_line = f"{datetime.now().isoformat()}\t{batch_id}\t{rows_written}\t{distinct_pages}\t{exact_count}\t{alias_count}\n"

        # Write header if file doesn't exist
        if not stats_path.exists():
            with open(stats_path, 'w') as f:
                f.write("batch_ts\tbatch_id\trows_written\tdistinct_wiki_pages_in_batch\tjoin_method_exact\tjoin_method_alias\n")

        # Append stats
        with open(stats_path, 'a') as f:
            f.write(stats_line)

        logger.info(
            f"Batch {batch_id}: {rows_written} rows, "
            f"{distinct_pages} unique pages, "
            f"exact={exact_count}, alias={alias_count}"
        )

    except Exception as e:
        logger.error(f"Error writing batch stats for batch {batch_id}: {e}")


def main():
    """Main streaming join pipeline."""
    args = parse_args()

    # Parse relevant keywords
    relevant_keywords = [kw.strip().lower() for kw in args.relevantCategories.split(',')]
    logger.info(f"Relevance keywords: {relevant_keywords}")

    # Create output directories
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    checkpoint_dir = Path(args.checkpoint)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("HTMLWikiTopicsJoin") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        # Load Wiki dimensions (batch/static)
        wiki_dir = Path(args.wiki)
        pages_df, aliases_df, categories_df, abstracts_df = load_wiki_dimensions(
            spark, wiki_dir, relevant_keywords
        )

        # Setup streaming source
        entities_stream = setup_streaming_source(
            spark, args.entities, args.maxFilesPerTrigger
        )

        # Perform join
        joined_df = perform_join(
            entities_stream,
            pages_df,
            aliases_df,
            categories_df,
            abstracts_df,
            relevant_keywords,
            args.absHit
        )

        # Write streaming output
        output_path = out_dir / "html_wiki_topics_output"
        stats_path = out_dir / "html_wiki_topics_stats.tsv"

        logger.info(f"Starting streaming query...")
        logger.info(f"Output dir: {output_path}")
        logger.info(f"Stats: {stats_path}")
        logger.info(f"Checkpoint: {checkpoint_dir}")

        # Use foreachBatch to write data + stats
        def process_batch(batch_df: DataFrame, batch_id: int):
            """Process each micro-batch."""
            if batch_df.count() > 0:
                # Write batch stats
                write_batch_stats(batch_df, batch_id, stats_path)

                # Write joined data
                batch_df.coalesce(1).write \
                    .mode("append") \
                    .option("sep", "\t") \
                    .option("header", "true") \
                    .csv(str(output_path))

        # Start streaming query
        query = joined_df.writeStream \
            .outputMode("append") \
            .foreachBatch(process_batch) \
            .option("checkpointLocation", str(checkpoint_dir)) \
            .trigger(processingTime='10 seconds') \
            .start()

        # Wait for termination
        logger.info("Streaming query started. Waiting for data...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming join: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
