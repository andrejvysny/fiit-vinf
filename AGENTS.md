# Repository Guidelines

## Project Structure
- Entry point: `bin/cli` orchestrates Spark pipelines; active code lives in `spark/jobs/` with helpers in `spark/lib/`.

## Documentation Lookup
- Always fetch framework/library docs via the Context7 MCP server; prioritize this for Apache Spark references and follow the retrieved guidance before implementing or changing code.

## Spark Coding Guidelines
- Use the Spark DataFrame API; avoid new code based on the legacy RDD API.

## Big Data Guidelines
- Design implementations for BIG DATA processing: prefer streaming and disk-first approaches.
- Never load entire datasets into driver or executor memory; avoid `.collect()`, `.toPandas()`, or any operation that materializes full datasets in memory.
- Use partitioned processing, lazy evaluation, and DataFrame / SQL transforms that operate per-partition.
- Persist intermediate large outputs to disk using efficient columnar formats (e.g., `Parquet`) and avoid keeping large intermediate state in memory.
- Tune Spark memory and enable spill-to-disk settings when appropriate; prefer incremental aggregations and windowed processing rather than full-group shuffles when possible.

## Libraries
- Allowed: native Python standard library and `PySpark` (tested against and targeted for `PySpark 3.4`).
- Disallowed: `pandas`, `numpy`, `dask`, or other dataframe/array libraries that encourage collecting large data into driver memory.
- Rationale: to ensure streaming/disk-first patterns and avoid accidental full-data materialization on the driver or executors.
- If you believe a specialised library is necessary, propose it in a PR and document how it preserves the big-data guarantees (no full collects, partition-aware processing, etc.).

## Running with `bin/cli`
- All runs must be executed via the repository entrypoint `bin/cli`, which orchestrates services through Docker Compose. Do not run Python scripts or Spark jobs locally unless explicitly authorised.
- Use `bin/cli` for common Spark tasks, for example:
	- `bin/cli extract` (use `--sample` for dry-runs)
	- `bin/cli wiki` and `bin/cli join` for Spark-based Wikipedia processing
	- `bin/cli join --sample 100` for a small join run

Notes:
- `bin/cli` handles launching the appropriate Docker Compose service and environment; prefer it to direct `docker compose` commands unless you need low-level control.
- If a local run is necessary for debugging, obtain explicit approval and document the reason in the corresponding PR or run log.

## Long-running Jobs & Monitoring
- Always assume Spark jobs can be long-running; when invoking runs or tests set an appropriate external timeout for monitoring and orchestration (CI, scripts, or supervisors).
- Use `bin/cli`/Docker Compose timeouts and health checks where possible; prefer monitoring via the Spark Web UI, container logs, and structured logs emitted by the task.
- If a run fails: collect relevant logs (`docker compose logs`, container stdout/stderr, Spark executor/driver logs), analyze the root cause, implement a fix, and rerun the job. Repeat this iterate-and-fix loop until the job completes successfully or you are explicitly instructed to stop.
- Document each failure and the corrective actions taken in the run log or PR description so reviewers can understand the remediation steps.

## Running with Docker Compose
- Spark services: `docker compose run --rm spark-extract`, `spark-wiki`, or `spark-join`.
- Tune Spark memory via env vars, e.g., `SPARK_DRIVER_MEMORY=8g SPARK_EXECUTOR_MEMORY=4g docker compose run --rm spark-extract`.
