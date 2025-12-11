# GEMINI_ANALYSIS.md

## 1. Overview

**Task:** Analyze why the Spark Wikipedia extraction job (`bin/cli wiki-full`) hangs during full runs despite successful sample runs (10k, 100k, 1M pages).

**Inspected Files:**
- `bin/cli` (Entrypoint and configuration)
- `spark/jobs/wiki_extractor.py` (Main Spark logic)
- `spark/lib/io.py` (I/O utilities)
- `docker-compose.yml` (Infrastructure and volumes)
- `working_multi.log` (Logs of successful sample runs)

## 2. Pipeline Reconstruction (Wiki Extraction)

The `wiki-full` command executes the following pipeline:

1.  **CLI Entry (`bin/cli wiki-full`)**:
    - Sets memory: Driver=14g, Executor=8g.
    - Sets `PARTITIONS=512`.
    - Sets `WIKI_ARGS="--force-spark --no-text --force"`.
    - Launches `vinf-spark-wiki` container via Docker Compose.

2.  **Spark Job (`spark/jobs/wiki_extractor.py`)**:
    - **Read:** Reads XML dump (100GB+) using `read_dump_as_dataframe`.
    - **Repartition:** Repartitions input to 512 partitions.
    - **Parse:** Uses `mapInPandas` + Python UDFs to extract text and metadata.
    - **Persist:** Calls `.persist(StorageLevel.DISK_ONLY)` on the `pages_with_text` DataFrame.
    - **Coalesce:** **Unconditionally** coalesces output DataFrames to 128 partitions (via `cap_partitions`).
    - **Write:** Writes 6 TSV files (`pages`, `categories`, etc.) to `workspace/store/wiki`.

## 3. Behaviour: Sample Runs vs Full Runs

| Feature | Sample Runs (10k - 1M) | Full Run (~6M pages) |
| :--- | :--- | :--- |
| **CLI Command** | `--sample N` (passes `-m N` to script) | No sample limit |
| **Input Reading** | Uses `.limit(N)`. Often collapses to **1 partition** (seen in logs). | Reads full dataset. Keeps 512 partitions. |
| **Persistence** | Persists small dataset to `/tmp/spark`. Fits in container overlay FS. | Persists **100GB+** to `/tmp/spark`. |
| **Outcome** | Success (21s - 7min). | **HANG**. Initial activity, then CPU/IO -> 0. |
| **Disk Usage** | Minimal. | Fills container root filesystem. |

**Evidence from Logs (`working_multi.log`):**
- Sample 100K: `Writing pages.tsv with 1 partitions` (Due to `.limit()` collapsing partitions).
- Full Run (hypothesized): Writes to `/tmp/spark` until space is exhausted.

## 4. Spark Implementation Review

### A. Critical Configuration Flaw (Docker)
In `docker-compose.yml`:
```yaml
  spark-wiki:
    environment:
      - SPARK_LOCAL_DIRS=/tmp/spark
    volumes:
      - ./wiki_dump:/opt/app/wiki_dump:ro
      - ./workspace/store/wiki:/opt/app/workspace/store/wiki
      - spark_checkpoints:/tmp/spark_checkpoints  # <--- Note this path
```
- **Issue:** `SPARK_LOCAL_DIRS` is set to `/tmp/spark`, but **no volume** is mounted at this location.
- **Consequence:** Spark writes spill files and persisted RDD data to the **container's overlay filesystem**. This filesystem is usually small (overlay2 default is often related to host /var/lib/docker, but without a mount, it lacks the performance and capacity of the project workspace).

### B. Code Logic: The Persistence Trap
In `spark/jobs/wiki_extractor.py`:
```python
    # CRITICAL: Persist pages_with_text to avoid re-running extract_page_udf
    log.info("Persisting parsed pages (pages_with_text) with DISK_ONLY...")
    pages_with_text = pages_with_text.persist(StorageLevel.DISK_ONLY)
```
- **Analysis:** This forces the *entire* parsed dataset (text + metadata) to be written to `SPARK_LOCAL_DIRS` before any output is completed. For the full English Wikipedia, this exceeds 100GB of uncompressed text.

### C. Code Logic: Unconditional Coalesce
In `spark/jobs/wiki_extractor.py`:
```python
        # Cap output partitions - but NOT for full extraction...
        def cap_partitions(df: DataFrame, cap: int = 128) -> DataFrame:
            ...
        # Coalesce all outputs to a manageable number of files
        pages_meta_df = cap_partitions(pages_meta_df)
```
- **Issue:** The comment says "NOT for full extraction", but the code **unconditionally** calls `cap_partitions`.
- **Consequence:** Even though `PARTITIONS=512` is set in the CLI, the job forces a shuffle/merge down to 128 partitions right before writing. This increases memory pressure on executors.

## 5. Root-Cause Hypotheses for Full-Run Stall

1.  **Container Disk Exhaustion (Primary Cause):**
    The `persist(DISK_ONLY)` writes 100GB+ of data to `/tmp/spark` inside the container. Since this is not a mounted volume, it fills the container's root filesystem. Once full, the executors block indefinitely waiting for I/O, causing CPU usage to drop to near zero and the job to hang.

2.  **I/O Bottleneck on OverlayFS:**
    Even if the disk doesn't fill completely, writing 100GB to a Docker overlay filesystem is extremely slow and resource-intensive compared to a bind mount or volume. This causes the "initial spike then drop" as buffers fill and processes enter `D` (uninterruptible sleep) state.

3.  **Coalesce Pressure:**
    Collapsing 512 partitions to 128 (via `cap_partitions`) forces each task to handle ~4GB+ of data (assuming 500GB uncompressed total). If Python worker reuse is enabled (`spark.python.worker.reuse=true`) and there's any memory leak in the UDF (e.g., regex cache), workers might stall or OOM.

## 6. Recommendations

### A. Infrastructure (Critical)
**Map `SPARK_LOCAL_DIRS` to a Volume.**
Update `docker-compose.yml` to mount a host directory or a named volume to `/tmp/spark`.
*Conceptual Change:*
```yaml
    volumes:
      - ./spark_temp:/tmp/spark  # Map to host disk
```
This ensures spill files and persisted data go to the host's large storage, not the container overlay.

### B. Code Logic (Important)
**Disable Coalescing for Full Runs.**
Modify `wiki_extractor.py` to respect the "no coalesce" intention for full runs.
*Conceptual Change:*
```python
    if args.wiki_max_pages is not None:
        # Only cap partitions for sample runs
        pages_meta_df = cap_partitions(pages_meta_df)
```

### C. Persistence Strategy (Optional)
**Review `DISK_ONLY` Necessity.**
If the storage volume is fast, `DISK_ONLY` is fine. If I/O is the bottleneck, consider removing `.persist()` and allowing Spark to recompute the UDFs (CPU bound) rather than choking on Disk I/O. However, given the Python UDF cost, fixing the volume mount (Recommendation A) is the correct first step.

## 7. Validation Plan

1.  **Verify Volume Mount:**
    - Inspect `docker-compose.yml` to ensure `/tmp/spark` is mounted.
    - Run `docker compose run --rm spark-wiki bash -c "df -h /tmp/spark"` to verify it has host-level capacity.

2.  **Run Medium Sample (e.g., 2M pages):**
    - `bin/cli wiki-full --sample 2000000`
    - Monitor disk usage in `./spark_temp` (or whatever volume is mounted) on the host.
    - Confirm it grows significantly but the job finishes.

3.  **Run Full Job:**
    - `bin/cli wiki-full`
    - Monitor `docker stats` to ensure CPU remains active (not dropping to 0%).
    - Monitor host disk space.
