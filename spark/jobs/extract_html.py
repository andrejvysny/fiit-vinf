"""PySpark extraction pipeline.

Spark SQL/DataFrame operations and joins follow the Spark SQL Programming
Guide (`https://github.com/apache/spark/blob/master/docs/sql-programming-guide.md`),
while scalar UDFs adhere to the PySpark UDF user guide
(`https://github.com/apache/spark/blob/master/python/docs/source/user_guide/udfandudtf.ipynb`).
The overall execution model uses the Spark overview guidance
(`https://spark.apache.org/docs/latest/`).
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark.lib.regexes import extract_all_entities
from spark.lib.tokenize import normalize_text, tokenize_with_positions
from spark.lib import io as io_utils
from spark.lib.utils import StructuredLogger, sha1_hexdigest, write_manifest


TOKEN_STRUCT = T.StructType(
    [
        T.StructField("tokens", T.ArrayType(T.StringType()), False),
        T.StructField("positions", T.ArrayType(T.IntegerType()), False),
    ]
)

ENTITY_STRUCT = T.StructType(
    [
        T.StructField("type", T.StringType(), False),
        T.StructField("value", T.StringType(), False),
        T.StructField("offsets_json", T.StringType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark-based text → index pipeline")
    parser.add_argument("--in", dest="input_root", default="data/text", help="Directory with *.txt files.")
    parser.add_argument("--meta", dest="meta_path", default="data/meta/docs.tsv", help="Metadata TSV.")
    parser.add_argument("--out", dest="index_dir", default="index", help="Output directory for index TSVs.")
    parser.add_argument("--entities", dest="entities_path", default="data/extract/entities.tsv", help="Entities TSV path.")
    parser.add_argument("--partitions", type=int, default=200, help="Repartition hint for wide stages.")
    parser.add_argument("--log", dest="log_path", default="logs/spark_extract.jsonl", help="Structured log path.")
    parser.add_argument("--stats", dest="stats_path", default="reports/spark_stats.tsv", help="Metrics TSV path.")
    parser.add_argument("--manifest-dir", default="runs", help="Directory for run manifests.")
    parser.add_argument("--run-id", default=None, help="Optional run identifier (defaults to UTC timestamp).")
    parser.add_argument("--sample", type=int, default=None, help="Limit number of text files for dry-runs.")
    return parser.parse_args()


def _relative_to(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def read_text_documents(spark: SparkSession, input_root: Path, partitions: int, limit: Optional[int]) -> DataFrame:
    root = input_root.resolve()
    sc = spark.sparkContext
    root_bc = sc.broadcast(str(root))

    # Loading raw text via wholeTextFiles follows the RDD Programming Guide:
    # https://github.com/apache/spark/blob/master/docs/rdd-programming-guide.md
    rdd = sc.wholeTextFiles(str(root), minPartitions=max(partitions, 1))

    def project(record: Tuple[str, str]) -> Tuple[str, str, str]:
        uri, content = record
        path = Path(uri.replace("file:", "")).resolve()
        rel = _relative_to(path, Path(root_bc.value))
        return path.stem, rel, content

    mapped = rdd.map(project)
    if limit is not None:
        mapped = mapped.zipWithIndex().filter(lambda kv: kv[1] < limit).map(lambda kv: kv[0])
    schema = T.StructType(
        [
            T.StructField("doc_id", T.StringType(), False),
            T.StructField("path_text", T.StringType(), False),
            T.StructField("text_raw", T.StringType(), False),
        ]
    )
    return spark.createDataFrame(mapped, schema=schema)


def read_metadata(spark: SparkSession, meta_path: Path) -> Optional[DataFrame]:
    if not meta_path.exists():
        return None
    df = (
        spark.read.format("csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .load(str(meta_path))
    )
    if "doc_id" not in df.columns:
        return None
    return df.select(F.col("doc_id").cast("string").alias("doc_id"), *[c for c in df.columns if c != "doc_id"])


def _tokenize_struct(text: str) -> Dict[str, List[int]]:
    tokens, positions = tokenize_with_positions(text)
    return {"tokens": tokens, "positions": positions}


def _entity_struct(text: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    for match in extract_all_entities(text):
        offsets = [{"start": match.start, "end": match.end, "source": "text"}]
        results.append({"type": match.entity_type, "value": match.value, "offsets_json": json.dumps(offsets, separators=(",", ":"))})
    return results


def main() -> int:
    args = parse_args()
    input_root = Path(args.input_root)
    meta_path = Path(args.meta_path)
    index_dir = Path(args.index_dir)
    entities_path = Path(args.entities_path)
    stats_path = Path(args.stats_path)
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_dir = Path(args.manifest_dir)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = manifest_dir / run_id / "manifest.json"

    start_ts = time.time()
    with StructuredLogger(Path(args.log_path)) as logger:
        logger.log("start", run_id=run_id, input=str(input_root), metadata=str(meta_path))

        builder = (
            SparkSession.builder.appName("spark-extract")
            .master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
        )
        # Session construction follows the Spark overview guidance: https://spark.apache.org/docs/latest/
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        text_df = read_text_documents(spark, input_root, args.partitions, args.sample)
        if text_df.rdd.isEmpty():
            logger.log("empty_input", path=str(input_root))
            spark.stop()
            return 0
        logger.log("text_loaded", rows=text_df.count())

        meta_df = read_metadata(spark, meta_path)
        if meta_df is not None:
            meta_df = meta_df.dropDuplicates(["doc_id"]).cache()
            logger.log("metadata_loaded", rows=meta_df.count())
        else:
            logger.log("metadata_missing", path=str(meta_path))

        normalize_udf = F.udf(normalize_text, T.StringType())
        # Scalar UDF per https://github.com/apache/spark/blob/master/python/docs/source/user_guide/udfandudtf.ipynb
        tokenize_udf = F.udf(_tokenize_struct, TOKEN_STRUCT)
        entity_udf = F.udf(_entity_struct, T.ArrayType(ENTITY_STRUCT))

        docs_df = (
            text_df
            .withColumn("text_norm", normalize_udf("text_raw"))
            .withColumn("tk", tokenize_udf("text_norm"))
            .withColumn("tokens", F.col("tk.tokens"))
            .withColumn("token_positions", F.col("tk.positions"))
            .drop("tk")
            .withColumn("doc_len", F.size("tokens"))
            .filter(F.col("doc_len") > 0)
        )

        if meta_df is not None:
            docs_df = docs_df.join(meta_df, on="doc_id", how="left")

        docs_df = docs_df.cache()
        doc_count = docs_df.count()
        total_tokens = docs_df.agg(F.sum("doc_len")).first()[0] or 0

        zipped = F.arrays_zip("tokens", "token_positions")
        exploded = (
            docs_df.select("doc_id", F.explode(zipped).alias("kv"))
            .select(
                "doc_id",
                F.col("kv.tokens").alias("term"),
                F.col("kv.token_positions").alias("position"),
            )
            .filter(F.col("term").isNotNull())
            .repartition(args.partitions, "term")
        )

        term_stats = (
            exploded.groupBy("term", "doc_id")
            .agg(
                F.count("*").alias("tf"),
                F.array_sort(F.collect_list("position")).alias("positions"),
            )
            .cache()
        )

        sample_ids = [row.doc_id for row in docs_df.select("doc_id").orderBy("doc_id").limit(100).collect()]
        if sample_ids:
            sample_tf = term_stats.filter(F.col("doc_id").isin(sample_ids))
            reference_tf = (
                exploded.filter(F.col("doc_id").isin(sample_ids))
                .groupBy("term", "doc_id")
                .agg(F.count("*").alias("ref_tf"))
            )
            parity_mismatches = (
                sample_tf.alias("new")
                .join(reference_tf.alias("ref"), on=["term", "doc_id"], how="outer")
                .filter(
                    (F.col("tf").isNull())
                    | (F.col("ref_tf").isNull())
                    | (F.col("tf") != F.col("ref_tf"))
                )
                .count()
            )
            logger.log("parity_check", sampled=len(sample_ids), mismatches=parity_mismatches)

        lexicon_df = (
            term_stats.groupBy("term")
            .agg(
                F.countDistinct("doc_id").alias("df"),
                F.sum("tf").alias("cf"),
            )
            .orderBy("term")
            .cache()
        )
        term_count = lexicon_df.count()

        postings_df = term_stats.select(
            "term",
            "doc_id",
            "tf",
            F.to_json("positions").alias("positions_json"),
        ).orderBy("term", "doc_id")

        avg_len = docs_df.agg(F.avg("doc_len")).first()[0] or 1.0
        docstore_df = (
            docs_df.select(
                "doc_id",
                "path_text",
                "text_norm",
                "doc_len",
            )
            .withColumn("title", F.substring_index("text_norm", " ", 24))
            .withColumn("lang", F.lit("und"))
            .select(
                "doc_id",
                "title",
                (F.col("doc_len") / F.lit(avg_len)).alias("norm_len"),
                "lang",
                "path_text",
            )
            .orderBy("doc_id")
        )

        entities_df = (
            docs_df.select("doc_id", entity_udf("text_norm").alias("entities"))
            .select("doc_id", F.explode("entities").alias("entity"))
            .select(
                "doc_id",
                F.col("entity.type").alias("type"),
                F.col("entity.value").alias("value"),
                F.col("entity.offsets_json").alias("offsets_json"),
            )
            .orderBy("doc_id")
        )

        index_dir.mkdir(parents=True, exist_ok=True)
        io_utils.write_tsv(lexicon_df, index_dir / "lexicon.tsv", ["term", "df", "cf"], header=True)
        io_utils.write_tsv(postings_df, index_dir / "postings.tsv", ["term", "doc_id", "tf", "positions_json"], header=True)
        io_utils.write_tsv(docstore_df, index_dir / "docstore.tsv", ["doc_id", "title", "norm_len", "lang", "path_text"], header=True)
        entities_path.parent.mkdir(parents=True, exist_ok=True)
        io_utils.write_tsv(entities_df, entities_path, ["doc_id", "type", "value", "offsets_json"], header=True)

        partitions_sizes = exploded.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        skew_max = max(partitions_sizes) if partitions_sizes else 0
        skew_min = min(partitions_sizes) if partitions_sizes else 0

        stats_rows = [
            ("documents", str(doc_count)),
            ("terms", str(term_count)),
            ("total_tokens", str(total_tokens)),
            ("avg_doc_length", f"{avg_len:.6f}"),
            ("max_partition_records", str(skew_max)),
            ("min_partition_records", str(skew_min)),
        ]
        stats_df = spark.createDataFrame(stats_rows, ["metric", "value"])
        io_utils.write_tsv(stats_df, stats_path, ["metric", "value"], header=True)

        outputs = {
            "lexicon": str(index_dir / "lexicon.tsv"),
            "postings": str(index_dir / "postings.tsv"),
            "docstore": str(index_dir / "docstore.tsv"),
            "entities": str(entities_path),
            "stats": str(stats_path),
        }
        sha_map = {name: sha1_hexdigest(Path(path)) for name, path in outputs.items()}

        manifest_payload = {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "inputs": {
                "text_root": str(input_root.resolve()),
                "metadata": str(meta_path.resolve()),
            },
            "outputs": outputs,
            "counts": {
                "documents": doc_count,
                "terms": term_count,
                "total_tokens": total_tokens,
            },
            "sha1": sha_map,
            "spark_conf": dict(spark.sparkContext.getConf().getAll()),
            "runtime_sec": time.time() - start_ts,
        }
        write_manifest(manifest_path, manifest_payload)
        logger.log("complete", run_id=run_id, documents=doc_count, terms=term_count, runtime=manifest_payload["runtime_sec"])
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
