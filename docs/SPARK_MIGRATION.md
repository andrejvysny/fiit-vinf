# Spark Extraction with Docker + PySpark

This document summarizes the completed migration that moves HTML → text/entity extraction from the single-process `extractor/` package into a PySpark job that runs inside Docker. The crawler (`python -m crawler`) and indexer (`python -m indexer.build`) continue to read/write the same `workspace/` artefacts, so downstream tooling is unchanged.

## Data Flow

```mermaid
flowchart LR
  A[crawler<br/>workspace/store/html] -->|bind mount| B[Docker Compose<br/>spark-master + spark-worker]
  B -->|spark-submit| C[html_extractor.py<br/>(PySpark job)]
  C -->|write_text() reuse| D[workspace/store/text]
  C -->|write_tsv() helper| E[workspace/store/entities/entities.tsv]
  D --> F[indexer.build CLI]
  E --> F
```

- Raw HTML stays on the host under `workspace/store/html/**.html`.
- `spark/jobs/html_extractor.py` loads config via `ExtractorConfig`, discovers HTML files with `HtmlFileDiscovery`, and parallelizes work across Spark partitions.
- Worker tasks call the existing `html_clean.html_to_text` and `entity_extractors.extract_all_entities` helpers so outputs match the legacy Python pipeline byte-for-byte.
- Per-document text files are written via `extractor.outputs.write_text`, preserving the mirrored directory layout expected by the indexer.
- Entity rows are materialized into a single TSV using `spark.lib.io.write_tsv`, which collapses Spark `part-*` files into `workspace/store/entities/entities.tsv`.

## Dockerized Runtime

`spark/docker-compose.yml` spins up a tiny standalone Spark cluster using the official `apache/spark-py:latest` image (Python-enabled Spark distribution). Services:

| Service | Purpose | Notes |
| --- | --- | --- |
| `spark-master` | Standalone master (`spark://spark-master:7077`) | Ports 7077/8080 follow the Spark standalone defaults from the official docs (`spark.master.port`, `spark.master.ui.port`). |
| `spark-worker` | Single worker with 4 vCores / 4 GiB | Shares the repo via `..:/app`, sets `PYTHONPATH=/app` so executors can import the repo modules. |
| `spark-job` | Ephemeral container for `spark-submit` | Mounts the repo, installs Python deps on demand, and submits `spark/jobs/html_extractor.py`. |

All services live on the `spark-net` bridge network. Because the repo is bind-mounted into each container, Spark workers see the same `workspace/` tree as the host, so writing per-document text files is safe even though the job executes inside Docker.

## CLI & Workflow

1. **Crawl (unchanged)**  
   ```bash
   python -m crawler --config config.yml
   ```
2. **Extract via Spark**  
   ```bash
   bin/spark_extract --config config.yml --limit 500
   ```
   - The wrapper activates the host venv, ensures Docker is available, starts `spark-master`/`spark-worker`, and then runs `docker compose run spark-job ...`.
   - Inside the container, we run `pip install -r requirements.txt` (fast after the first invocation) and call `spark-submit --master spark://spark-master:7077 /app/spark/jobs/html_extractor.py ...`.
   - Pass any extractor flags (`--force`, `--no-text`, `--no-entities`, `--sample`, etc.); they are forwarded directly to the PySpark job.
   - Use `bin/spark_extract --local ...` to fall back to the original pure-Python CLI if Docker isn’t available.
3. **Index (unchanged)**  
   ```bash
   python -m indexer.build --config config.yml
   ```
4. **Shut down Spark when finished**  
   ```bash
   docker compose -f spark/docker-compose.yml down
   ```

## Job Internals

- `spark/jobs/html_extractor.py` builds a `SparkSession` with `.master(args.master)` (defaulting to `SPARK_MASTER_URL`, i.e. `spark://spark-master:7077`) and configures UTC timestamps.
- HTML discovery stays on the driver (`HtmlFileDiscovery.discover(limit=...)`) so we have deterministic ordering and honour `--limit`/`--sample`.
- Work is sharded with `parallelize(files, partitions)` and processed via `_process_partition`. Each partition:
  - Skips files whose text artefacts already exist when `--no-entities` is set and `--force` is false (parity with `HtmlFileProcessor`).
  - Reads HTML from disk, normalizes text via `html_clean.html_to_text(..., strip_boilerplate=False)`, and writes using `write_text`.
  - Extracts entities with `entity_extractors.extract_all_entities` and tracks per-type flags (stars, forks, languages, topics, URLs, emails, README, license) for summary stats.
- Stats are reduced across partitions and logged as a dictionary so we can spot regressions quickly.
- Entities are converted into a Spark DataFrame with a fixed schema and written to a single TSV file. The helper sanitizes tabs/newlines the same way as `EntitiesOutput`.

## Testing & Parity

- `tests/test_spark_extractor.py` executes `_process_partition` on a temp workspace without needing a running Spark cluster. It asserts that:
  - The text file mirrors the layout/content produced by the Python pipeline.
  - Extracted entity tuples exactly match `entity_extractors.extract_all_entities(...)`.
- `python -m unittest discover tests` remains the canonical regression suite and now covers both the regex helpers and the Spark partition glue.

## Troubleshooting

- **Docker/Spark not running** — `bin/spark_extract` ensures `spark-master`/`spark-worker` are up, but you can manually inspect via `docker compose -f spark/docker-compose.yml ps`.
- **Stopping services** — Use `docker compose -f spark/docker-compose.yml down` to free ports 7077/8080/8081.
- **Changing resources** — Edit `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` in `spark/docker-compose.yml` or scale workers via `docker compose up --scale spark-worker=2`.
- **Running locally** — `bin/spark_extract --local ...` calls the legacy Python extractor for quick smoke tests without Docker.

With these pieces in place, the heavy extraction logic now leverages Spark’s parallelism while keeping the old CLI ergonomics, workspace layout, and downstream contracts intact.
