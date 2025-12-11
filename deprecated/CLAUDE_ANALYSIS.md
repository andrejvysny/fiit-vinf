# Wikipedia Extraction Pipeline Analysis Report

**Agent**: Claude Opus 4.5
**Date**: 2025-12-11
**Task**: Diagnose why full Wikipedia dump extraction hangs while sampled runs complete successfully

---

## 1. Overview

This report analyzes the Apache Spark Wikipedia extraction pipeline to explain why full-dump runs hang while sampled runs (10K, 100K, 1M pages) complete successfully.

### Files and Logs Inspected

| Category | Files |
|----------|-------|
| Documentation | `AGENTS.md`, `GEMINI.md`, `CLAUDE.md` |
| Implementation | `spark/jobs/wiki_extractor.py`, `spark/lib/wiki_regexes.py`, `spark/lib/io.py`, `spark/lib/progress.py` |
| Configuration | `bin/cli`, `docker-compose.yml`, `config.yml` |
| Success Logs | `working_multi.log`, `logs/wiki_extract.jsonl` |
| Failure Logs | `error_full_.log` |

---

## 2. Pipeline Reconstruction (Wiki Extraction)

### End-to-End Data Flow

```
bin/cli wiki-full [--sample N]
       │
       ▼
docker compose run spark-wiki
       │
       ▼
spark-submit wiki_extractor.py
       │
       ├──► Native Python mode (if sample <= 50,000)
       │    └── Sequential file read → pandas processing → TSV write
       │
       └──► Spark DataFrame mode (if sample > 50,000 or full)
            │
            ├─1─► spark.read.text(dump.xml)          # Read as lines
            │     └── 831 partitions for 111.5GB file
            │
            ├─2─► mapInPandas(extract_pages)         # Parse <page> blocks
            │     └── pages_df.persist(DISK_ONLY)
            │
            ├─3─► UDF transformations                 # Parse XML content
            │     ├── extract_page_udf
            │     ├── normalize_title_udf
            │     ├── extract_categories_udf
            │     ├── extract_links_udf
            │     ├── extract_infobox_udf
            │     └── extract_abstract_udf
            │     └── pages_with_text.persist(DISK_ONLY)
            │
            ├─4─► cap_partitions(df, 128)            # CRITICAL: Coalesce
            │     └── 831 → 128 partitions
            │
            └─5─► write_tsv() for each output        # 6 TSV files
                  ├── pages.tsv
                  ├── categories.tsv
                  ├── links.tsv
                  ├── infobox.tsv
                  ├── abstract.tsv
                  └── aliases.tsv
```

### Key Stages

| Stage | Operation | Input Partitions | Output Partitions | Location |
|-------|-----------|------------------|-------------------|----------|
| 1 | Read XML lines | N/A | 831 | `wiki_extractor.py:540` |
| 2 | Extract page blocks | 831 | 831 | `wiki_extractor.py:575` |
| 3 | Parse + UDFs | 831 | 831 | `wiki_extractor.py:611-623` |
| 4 | Coalesce for output | 831 | 128 | `wiki_extractor.py:1031-1040` |
| 5 | Write TSV | 128 | N/A | `wiki_extractor.py:1048` |

---

## 3. Behaviour: Sample Runs vs Full Runs

### Successful Sample Runs (from `working_multi.log`)

| Sample Size | Duration | Pages/sec | Partitions | Memory Config | Status |
|-------------|----------|-----------|------------|---------------|--------|
| 10,000 | 21.7s | 352 | 16 | 14g driver, 8g executor | Complete |
| 100,000 | 105.3s | 738 | 831 (kept) | 14g driver, 8g executor | Complete |
| 1,000,000 | 420.0s | 1,852 | 831 (kept) | 14g driver, 8g executor | Complete |
| 3,000,000 | 1,138.6s | 2,052 | 831 (kept) | 14g driver, 8g executor | Complete |

**Key observations from successful runs:**
- 10K sample uses 16 partitions (coalesced down from 831)
- 100K+ samples keep original 831 partitions
- Progress reporter shows consistent stage completion
- No Python worker crashes

### Failed Full Runs (from `error_full_.log` and `logs/wiki_extract.jsonl`)

| Attempt | Duration Before Failure | Progress | Failure Mode |
|---------|------------------------|----------|--------------|
| 1 | ~22 minutes | 32/128 partitions | Python worker crash |
| 2 | ~18 minutes | Unknown | `Connection reset` during csv write |
| 3 | ~20 minutes | Unknown | `o320.csv` error |
| 4 | ~7 minutes | Unknown | `o323.csv` error |

**Key observations from failing runs:**

1. **Initial progress, then stall**: Jobs complete 25-32 partitions (~20-25%) before crashing
2. **Python worker crashes**: `ERROR PythonUDFRunner: Python worker exited unexpectedly (crashed)`
3. **Socket errors**: `java.net.SocketException: Connection reset`
4. **Block manager failures**: `WARN BlockManager: Putting block rdd_23_83 failed`
5. **Task write failures**: `[TASK_WRITE_FAILED] Task failed while writing rows`

**Error timeline from `error_full_.log`:**
```
10:52:06 - Job starts, 256 partitions configured
10:52:07 - MemoryStore: 11.4 GiB capacity
10:52:11 - Coalescing output: 831 → 128 partitions
10:58:09 - First tasks complete: 6/128 done
11:06:27 - Stalled at 25/128 done (14 active, 0 failed)
11:13:57 - CRASH: Python worker exited unexpectedly
11:13:57 - Connection reset on PythonUDFRunner
11:14:01 - Job aborted due to stage failure
```

---

## 4. Spark Implementation Review

### 4.1 Architecture Design (Correct)

The pipeline follows streaming principles:
- Uses `spark.read.text()` for line-by-line reading
- Uses `mapInPandas` for page extraction (iterator-based)
- Uses `DISK_ONLY` persistence to avoid RAM pressure
- Writes partitioned outputs (not coalesced to single file by default)

### 4.2 Identified Anti-Patterns

#### CRITICAL: `cap_partitions()` Coalesce Operation

**Location**: `wiki_extractor.py:1031-1040`

```python
def cap_partitions(df: DataFrame, cap: int = 128) -> DataFrame:
    current = df.rdd.getNumPartitions()
    target = min(cap, current)
    if target < current:
        logger.info(f"Coalescing output partitions: {current} -> {target}")
        return df.coalesce(target)
    return df
```

**Problem**: Coalescing from 831 to 128 partitions means each output task processes data from ~6.5 input partitions. Since Python UDFs (categories, links, infobox, etc.) run during the write phase, each output task must:

1. Read from multiple persisted DISK_ONLY partitions
2. Execute Python UDFs on all that data
3. Hold results in Python worker memory
4. Write to output file

**Impact**: With 6M pages across 831 partitions (~7,200 pages/partition), coalescing to 128 means each output task handles ~47,000 pages worth of UDF processing. This overwhelms Python worker memory (default 512MB-1GB).

#### HIGH: Multiple Python UDFs on Hot Path

**Location**: `wiki_extractor.py:596-608`

```python
extract_page_udf = F.udf(extract_page_xml, MapType(StringType(), StringType()))
normalize_title_udf = F.udf(normalize_title, StringType())
extract_categories_udf = F.udf(extract_categories, ArrayType(StringType()))
extract_links_udf = F.udf(extract_internal_links, ArrayType(StringType()))
extract_infobox_udf = F.udf(extract_infobox_fields, MapType(StringType(), StringType()))
extract_abstract_udf = F.udf(extract_abstract, StringType())
```

**Problem**: Each UDF:
- Spawns a Python subprocess
- Serializes/deserializes data via Py4J sockets
- Creates Python objects for each row processed
- These UDFs are "re-evaluated" when reading from DISK_ONLY cache during write

#### MEDIUM: Persistence Strategy Gap

**Location**: `wiki_extractor.py:1006`, `wiki_extractor.py:636`

```python
pages_df = pages_df.persist(StorageLevel.DISK_ONLY)
# ...
pages_with_text = pages_with_text.persist(StorageLevel.DISK_ONLY)
```

**Issue**: While `pages_df` and `pages_with_text` are persisted, the derived DataFrames (categories_df, links_df, etc.) are NOT persisted before coalescing and writing. This means:

1. When `cap_partitions(categories_df)` is called, it triggers a lazy evaluation chain
2. The UDFs run during the coalesced write phase
3. Each output partition must process ~6.5 input partitions' worth of UDF work

#### MEDIUM: Python Worker Memory Configuration

**Location**: `wiki_extractor.py:940`

```python
python_worker_memory = os.environ.get("SPARK_PYTHON_WORKER_MEMORY", "1g")
```

**Issue**: Default 1GB per worker is insufficient when each worker processes 47,000 pages of XML parsing output.

### 4.3 Why Sample Runs Succeed

| Sample Size | Behavior | Why It Works |
|-------------|----------|--------------|
| 10K | Coalesce 16→16 (no change) | Small partition size |
| 100K | Keep 831 partitions | ~100 pages/partition |
| 1M | Keep 831 partitions | ~1,200 pages/partition |
| 3M | Keep 831 partitions | ~3,600 pages/partition |

For samples, each partition has manageable data volume. Python workers can process the UDFs without OOM.

### 4.4 Why Full Runs Fail

| Factor | Full Run Value | Impact |
|--------|----------------|--------|
| Total pages | ~6M | Very large dataset |
| Input partitions | 831 | ~7,200 pages/partition |
| Output partitions | 128 | ~47,000 pages/task |
| UDF invocations/task | ~47,000 | Memory exhaustion |
| Python worker memory | 1GB | Insufficient |

The coalesce operation (831→128) is the critical trigger. Each output task must:
1. Read serialized data from ~6.5 DISK_ONLY partitions
2. Deserialize through Py4J
3. Execute UDFs (XML parsing, regex extraction)
4. Hold intermediate results in Python heap
5. Serialize output for writing

Steps 1-4 overwhelm the Python worker's memory allocation, causing crashes.

---

## 5. Root-Cause Hypotheses for Full-Run Stall

### Hypothesis 1: Coalesce-Induced Python Worker OOM (PRIMARY)

**Confidence**: 95%

**Evidence**:
- `error_full_.log:173`: `ERROR PythonUDFRunner: Python worker exited unexpectedly (crashed)`
- `error_full_.log:174`: `java.net.SocketException: Connection reset`
- Jobs crash at ~25% completion (32/128 partitions)
- Error occurs during `csv` write phase (when coalesced UDFs execute)

**Mechanism**:
1. `cap_partitions()` coalesces 831→128 partitions
2. Each output task must process ~6.5x more data
3. UDFs (extract_categories, extract_links, etc.) run during write
4. Python workers OOM and crash
5. JVM detects socket reset, aborts task
6. After 1 retry failure, stage aborts

**Why sample runs work**:
- 10K: No coalesce (16 partitions kept)
- 100K-3M: 831 partitions kept, each handles manageable load

### Hypothesis 2: DISK_ONLY Spill Overhead + UDF Replay

**Confidence**: 80%

**Evidence**:
- DISK_ONLY persistence means data is serialized to disk
- Reading back triggers deserialization + UDF re-execution
- `error_full_.log:92`: MemoryStore shows 11.4 GiB capacity but heavy disk spill

**Mechanism**:
1. `pages_with_text` is persisted with DISK_ONLY
2. When reading for output writes, data is deserialized
3. UDFs on derived columns are re-executed (lazy evaluation)
4. This doubles the UDF execution work per output task

### Hypothesis 3: Skewed Partitions

**Confidence**: 60%

**Evidence**:
- Progress logs show some partitions complete quickly (0→13 in 7 min)
- Then stalls (13→25 takes 8 min, 25→32 takes 8 min)
- Some Wikipedia pages are very large (discussion pages, lists)

**Mechanism**:
1. XML dump partitioning is byte-based, not page-based
2. Some partitions may contain very large pages
3. UDF processing time varies significantly
4. Coalescing amplifies skew (combines slow + fast partitions)

### Hypothesis 4: GC Pressure from Map/Array UDF Returns

**Confidence**: 50%

**Evidence**:
- UDFs return complex types: `MapType`, `ArrayType`
- These create many Python objects
- Kryo serialization shows in error traces

**Mechanism**:
1. `extract_infobox_udf` returns `MapType(StringType(), StringType())`
2. `extract_categories_udf` returns `ArrayType(StringType())`
3. Each page creates multiple Python dict/list objects
4. GC cannot keep up with allocation rate during coalesced tasks

### Hypothesis 5: Docker Container Memory Limit

**Confidence**: 40%

**Evidence**:
- `docker-compose.yml:153`: `memory: ${CONTAINER_MEMORY_LIMIT:-16g}`
- Driver (14g) + Executor (8g) + Off-heap (4g) = 26GB > 16GB limit

**Mechanism**:
1. Container has 16GB limit
2. JVM heap exceeds container memory
3. Container killed by Docker OOM killer
4. Appears as sudden disconnect

---

## 6. Recommendations (Conceptual Only)

### R1: Remove or Disable `cap_partitions()` for Full Runs

**Where**: `wiki_extractor.py:1031-1040`

**Change**: Skip coalescing when `wiki_max_pages is None` (full extraction)

```python
# Conceptual change:
if not is_full_extraction:
    df = cap_partitions(df, 128)
# For full runs, keep original 831 partitions
```

**Reference**: Spark Tuning Guide recommends avoiding coalesce before heavy operations

### R2: Persist Derived DataFrames Before Write

**Where**: `wiki_extractor.py:1060-1080`

**Change**: Persist `categories_df`, `links_df`, etc. with DISK_ONLY before writing

```python
# Conceptual change:
categories_df = categories_df.persist(StorageLevel.DISK_ONLY)
# Trigger materialization
categories_df.count()
# Then write (no re-execution of UDFs)
write_tsv(categories_df, ...)
```

**Reference**: Spark RDD Programming Guide on persistence

### R3: Increase Python Worker Memory

**Where**: `wiki_extractor.py:940`, `docker-compose.yml:109`

**Change**: Set `SPARK_PYTHON_WORKER_MEMORY=4g` for full runs

```yaml
# docker-compose.yml conceptual change:
environment:
  - SPARK_PYTHON_WORKER_MEMORY=${PYTHON_WORKER_MEMORY:-4g}
```

### R4: Use `mapInPandas` Instead of UDFs for Extraction

**Where**: `wiki_extractor.py:596-608`

**Change**: Replace individual UDFs with a single `mapInPandas` call that processes all extractions in one pass

**Reference**: PySpark API docs on `mapInPandas` for batch processing

### R5: Increase Container Memory Limit

**Where**: `docker-compose.yml:153`

**Change**: Set `CONTAINER_MEMORY_LIMIT=24g` or higher

### R6: Add Partition-Level Checkpointing

**Where**: After UDF transformations

**Change**: Checkpoint to break lineage before heavy writes

```python
# Conceptual:
spark.sparkContext.setCheckpointDir("/tmp/spark_checkpoints")
pages_with_text = pages_with_text.checkpoint()
```

---

## 7. Validation Plan

### Step 1: Validate Fix with 10K Sample

```bash
bin/cli wiki-full --sample 10000 --force
```

**Metrics to watch**:
- Completion time (baseline: 21.7s)
- Python worker errors: should be 0
- Memory usage via `docker stats`

**Success criteria**: Completes without error

### Step 2: Validate with 1M Sample (Pre-Fix Baseline)

```bash
bin/cli wiki-full --sample 1000000 --force
```

**Metrics**:
- Duration (baseline: 420s)
- Spark UI: check partition sizes
- `docker stats`: memory usage

**Success criteria**: Completes in ~7 minutes

### Step 3: Validate with 3M Sample

```bash
bin/cli wiki-full --sample 3000000 --force
```

**Metrics**:
- Duration (baseline: 1138s)
- Watch for any Python worker warnings
- Monitor `logs/wiki_extract.jsonl` for errors

**Success criteria**: Completes in ~19 minutes

### Step 4: Monitored Full Run

```bash
# Terminal 1: Run extraction
SPARK_PYTHON_WORKER_MEMORY=4g PARTITIONS=831 bin/cli wiki-full --force

# Terminal 2: Monitor Spark UI
open http://localhost:4040

# Terminal 3: Monitor container resources
docker stats vinf-spark-wiki
```

**Metrics to watch**:
- Spark UI: Active stages, task distribution
- Memory: Should stay below container limit
- Progress: Should continue past 25% (32/128)
- Logs: No "Python worker exited unexpectedly"

**Success criteria**:
- Completes all 831 partitions
- Duration: 2-4 hours for 6M pages
- No Python worker crashes

### Step 5: Verify Outputs

```bash
# Check output sizes
ls -lh workspace/store/wiki/*.tsv

# Verify row counts
wc -l workspace/store/wiki/pages.tsv
# Expected: ~6M lines

# Validate TSV structure
head -5 workspace/store/wiki/pages.tsv
```

---

## 8. References

### Spark Documentation Consulted

- [Spark Tuning Guide](https://spark.apache.org/docs/latest/tuning.html) - Persistence, partitioning
- [PySpark UDF Performance](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html) - UDF best practices
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) - Shuffle partitions

### Project Documentation

- `CLAUDE.md`: Pipeline architecture, "no `.cache()`" rule for wiki jobs
- `AGENTS.md`: Memory configuration guidance
- `GEMINI.md`: Additional context on Spark configuration

---

## 9. Summary

The Wikipedia extraction pipeline hangs on full runs due to the `cap_partitions()` function coalescing 831 input partitions to 128 output partitions. This forces each output task to execute Python UDFs on ~6.5x more data than originally partitioned, causing Python worker OOM crashes.

**Key Findings**:

1. **Primary Cause**: `coalesce(128)` on 831-partition DataFrames
2. **Trigger**: Python UDFs execute during coalesced write phase
3. **Symptom**: Workers crash at ~25% progress (32/128 partitions)
4. **Error**: `PythonUDFRunner: Python worker exited unexpectedly`

**Critical Fix**: Do not coalesce output partitions for full extraction runs. Keep the original 831 partitions to maintain manageable per-task workload.

---

*Report generated by Claude Opus 4.5 analysis agent*
