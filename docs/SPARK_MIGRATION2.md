# SPARK_MIGRATION

## Environment & Entrypoints

- Use the project’s virtualenv and pin PySpark to the latest 4.0.x line that matches the system Python: `python -m venv venv && source venv/bin/activate && pip install -r requirements.txt pyspark`.
- All Spark invocations run in local mode with UTC timezone or via the Dockerized `spark://` master. The helper script `bin/spark_extract` activates the venv, exports `PYSPARK_PYTHON`, and wraps the `spark-submit` invocation (`bin/spark_extract`).
- The main job lives in `spark/jobs/html_extractor.py` and orchestrates text loading, tokenization, regex entity extraction, and TSV writers. Supporting modules under `spark/lib/` provide reusable tokenization, regex bundles, and TSV/NDJSON IO.

## Architecture

```mermaid
flowchart LR
  A[data/meta/docs.tsv<br/>(JSONL supported)] --> B[Load WholeTextFiles<br/>+ metadata count]
  A2[data/text/**/*.txt] --> B
  B --> C[Normalize + tokenize<br/>UDF from spark/lib/tokenize.py]
  C --> D[Regex extract entities<br/>spark/lib/regexes.py]
  C --> E[Per-doc stats<br/>tf_raw, len, norm_len]
  E --> F[Global stats<br/>df, cf]
  F --> G[index/lexicon.tsv]
  E --> H[index/postings.tsv]
  C --> I[index/docstore.tsv]
  D --> J[data/extract/entities.tsv]
  B --> K[reports/spark_stats.tsv<br/>logs/spark_extract.jsonl]
  K --> L[runs/<ts>/manifest.json]
```

## Components

| Component | Purpose / Notes |
| --- | --- |
| `spark/lib/tokenize.py` | Normalizes Markdown/HTML, folds accents, and emits tokens + offsets for Spark UDFs (`spark/lib/tokenize.py:1-72`). |
| `spark/lib/regexes.py` | Recompiled entity patterns with bounded attribute scanning, plus helpers to emit `EntityMatch` objects (`spark/lib/regexes.py:1-240`). |
| `spark/lib/io.py` | UTF-8 TSV/NDJSON readers/writers that collapse Spark outputs to single files (`spark/lib/io.py:1-112`). |
| `spark/lib/utils.py` | SHA1 helper, manifest writer, and structured JSONL logger (`spark/lib/utils.py:1-42`). |
| `spark/jobs/extract_html.py` | CLI parsing, SparkSession creation, text ingestion via `wholeTextFiles`, repartitioning, TF/DF aggregation, TSV emission, logs, stats, and manifests (`spark/jobs/extract_html.py:1-308`). |
| `bin/spark_extract` | POSIX wrapper that sources the virtualenv and launches the job with sensible defaults. |

## Schemas & Outputs

| File | Columns | Notes |
| --- | --- | --- |
| `index/lexicon.tsv` | `term`, `df`, `cf` | Sorted lexicon, coalesced to a single TSV (`spark/jobs/extract_html.py:219-249`). |
| `index/postings.tsv` | `term`, `doc_id`, `tf`, `positions_json` | Contains positional lists encoded as JSON per document to support phrase queries. |
| `index/docstore.tsv` | `doc_id`, `title`, `norm_len`, `lang`, `path_text` | `norm_len = length / avg_len` underpins TF-IDF-1 and BM25-lite score normalization. |
| `data/extract/entities.tsv` | `doc_id` (SHA key), `type`, `value`, `offsets_json` | Recomputed against the text currently indexed, keeping offsets in sync with postings. |
| `logs/spark_extract.jsonl` | `ts`, `event`, `payload...` | Structured telemetry for each run (`spark/jobs/extract_html.py:118-208`). |
| `reports/spark_stats.tsv` | `metric`, `value` | Document/term counts, token totals, partition skew, metadata rows (`spark/jobs/extract_html.py:257-267`). |
| `runs/<ts>/manifest.json` | inputs, outputs, counts, Spark conf, SHA-1s | Immutable manifest for reproducibility (`spark/jobs/extract_html.py:269-306`). |

## Code Highlights

- **Session & ingestion:** The job builds a `SparkSession` with UTC timezone and loads text via `SparkContext.wholeTextFiles`, then deterministically captures `doc_id`/paths entirely inside Spark (`spark/jobs/extract_html.py:139-171`). This follows the Spark SQL DataFrame guidance for creating structured tables from JVM objects and RDDs [Spark SQL Programming Guide — DataFrame Operations](https://github.com/apache/spark/blob/master/docs/sql-programming-guide.md).
- **Tokenization UDF:** A pure-Python tokenizer is wrapped as a PySpark UDF so DataFrame columns can be exploded without leaving the JVM boundary (`spark/jobs/extract_html.py:200-214`). The design mirrors the official UDF guidance, including Arrow-friendly signatures when enabled later [PySpark UDF and UDTF User Guide](https://github.com/apache/spark/blob/master/python/docs/source/user_guide/udfandudtf.ipynb).
- **Term aggregation:** `arrays_zip` + `explode` capture token positions, and the job repartitions by `term` before grouping so wide shuffles stay balanced (`spark/jobs/extract_html.py:216-233`). Positions are array-sorted and converted to JSON strings for TSV emission.
- **Regex extraction:** `extract_entities()` runs per-partition in Python, leveraging the bounded regex bundle to avoid catastrophic backtracking, then writes the familiar TSV header (`spark/jobs/extract_html.py:235-254`, `spark/lib/regexes.py:40-240`).
- **Writers & manifests:** `spark/lib/io.write_tsv` writes to temp directories and collapses the single part file into the requested path, ensuring deterministic filenames for downstream CLIs (`spark/lib/io.py:31-47`). SHA-1s are recorded in the manifest for auditability (`spark/jobs/extract_html.py:269-306`).

## Partitioning & Skew Mitigation

- Text ingestion uses `wholeTextFiles(..., minPartitions=args.partitions)` plus a broadcasted root path to keep relative filenames without materializing data on the driver (`spark/jobs/extract_html.py:139-171`). This taps into the core RDD transformation APIs described in the RDD Programming Guide [Spark RDD Programming Guide — Transformations](https://github.com/apache/spark/blob/master/docs/rdd-programming-guide.md).
- Token explosions are repartitioned by `term` (`repartition(args.partitions, "term")`) before aggregations, preventing single-term hotspots from dominating a partition.
- The job records per-partition record counts and writes them to `reports/spark_stats.tsv` so operators can tune `--partitions` in future runs.

## Ranker Compatibility

- `index/docstore.tsv` stores `norm_len`, enabling BM25-lite scoring in the query layer: `idf2 = log((N - df + 0.5)/(df + 0.5) + 1)` with `k1=1.2`, `b=0.75`.
- Lexicon/postings TSVs expose all ingredients required for TF-IDF-1 (`tf = 1 + log(tf_raw)`, `idf1 = log((N + 1)/(df + 1)) + 1`). Downstream rankers can stream these TSVs without JSON decoding.

## CLI Example

```bash
bin/spark_extract \
  --in data/text \
  --meta data/meta/docs.tsv \
  --out index \
  --entities data/extract/entities.tsv \
  --partitions 256
```

The command writes logs (`logs/spark_extract.jsonl`), stats (`reports/spark_stats.tsv`), index TSVs under `index/`, entity TSVs, and a fresh manifest under `runs/<timestamp>/manifest.json`.

## Validation Plan

1. **Document counts:** Compare `reports/spark_stats.tsv` vs. `wc -l store/text/**/*.txt` to ensure no documents were dropped.
2. **Term stats parity:** Sample `k` documents, recompute TF/DF with the legacy `indexer` to confirm TSV values differ only when normalization settings changed (document in manifest).
3. **Ranker smoke-test:** Run five representative queries with both TF-IDF-1 and BM25-lite once the search layer consumes the TSV index, comparing top-10 overlap and score trends.
4. **Entity sanity:** Spot-check `data/extract/entities.tsv` against historical outputs to confirm offsets now align with indexed text.
5. **Skew & perf:** Inspect `reports/spark_stats.tsv` (`max_partition_records`) and `logs/spark_extract.jsonl` runtime entries to adjust `--partitions` or input layout as needed.

With these artefacts and scripts in place, migrating the crawler/extractor/indexer workflow to PySpark local mode preserves the filesystem-only contract while unlocking parallel normalization, deterministic TSV indices, and richer manifests/logs for future automation.
