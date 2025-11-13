# Spark Extraction Overview

## Docker Configuration

**Image**: `apache/spark-py:latest` (platform: `linux/amd64` for macOS ARM64 compatibility)

**Key Environment Variables**:
- `SPARK_DRIVER_MEMORY`: 6g default (4g in command, overridden by env)
- `SPARK_EXECUTOR_MEMORY`: 4g default
- `SPARK_LOCAL_DIRS=/tmp/spark`
- `PYTHONPATH=/opt/app`
- `PYSPARK_PYTHON=python3`

**Execution**: `spark-submit --master local[*]` (uses all CPU cores)

## Spark Jobs

### 1. HTML Extraction (`spark/jobs/html_extractor.py`)

**Purpose**: Extract text and entities from HTML files in parallel

**Process**:
- Discovers HTML files recursively from `workspace/store/html`
- Partitions files across Spark workers (default: 64 partitions)
- Each partition processes files using `extractor.html_clean.html_to_text()` and `extractor.entity_extractors.extract_all_entities()`
- Writes text files to `workspace/store/spark/text/`
- Writes entities TSV to `workspace/store/spark/entities/entities.tsv`

**Key Features**:
- Skips existing text files unless `--force`
- Broadcasts configuration to workers
- Aggregates statistics (files processed, entities extracted, etc.)

**Entry Point**: `bin/spark_extract` (wrapper script with `--local` fallback)



### 2. Wikipedia Extraction (`spark/jobs/wiki_extractor.py`)

**Purpose**: Extract structured data from Wikipedia XML dumps

**Process**:
- Reads Wikipedia dump files (`.xml` or `.xml.bz2`) with streaming approach
- Extracts `<page>` blocks without excessive buffering
- Uses user defined functions to parse XML and extract:
  - Pages metadata (page_id, title, namespace, redirects, timestamp)
  - Categories (normalized)
  - Internal links (normalized)
  - Infobox fields (key-value pairs)
  - Abstracts (first paragraph)
  - Redirect aliases
  - Full text (optional, deduplicated by SHA256)

**Outputs** (TSV files in `workspace/store/wiki/`):
- `pages.tsv`: Page metadata
- `categories.tsv`: Page→category mappings
- `links.tsv`: Page→internal link mappings
- `infobox.tsv`: Page→infobox field mappings
- `abstract.tsv`: Page abstracts
- `aliases.tsv`: Redirect alias→canonical mappings
- `wiki_text_metadata.tsv`: Page→SHA256→text file mappings
- `text/`: Directory with text files named by SHA256 hash

**Memory Tuning**: Auto-scales partitions (64→256) for files >50GB

**Entry Point**: `bin/spark_wiki_extract`



### 3. HTML-Wiki Join (`spark/jobs/join_html_wiki.py`)

**Purpose**: Join HTML entities with Wikipedia canonical data

**Process**:
- Loads entities TSV and Wikipedia TSV files
- Builds canonical mapping (direct pages + aliases)
- Normalizes entity values using `normalize_title()` UDF
- Performs left join on normalized values
- Calculates confidence scores (direct match: +0.2, alias: +0.1, case match: +0.1)
- Writes join results, statistics, and per-document aggregates

**Supported Entity Types**: `LANG_STATS`, `LICENSE`, `TOPICS`, `README`

**Entry Point**: `bin/spark_join_wiki`

## Execution Flow

1. **Wrapper Script** (`bin/spark_extract`, `bin/spark_wiki_extract`, etc.):
   - Parses arguments
   - Sets memory based on data size
   - Runs `docker compose -f docker-compose.spark.yml run --rm spark`

2. **Docker Container**:
   - Installs Python dependencies (`pip install -r requirements.txt`)
   - Executes `spark-submit` with job script and arguments

3. **Spark Job**:
   - Creates SparkSession with `local[*]` master
   - Processes data in parallel partitions
   - Writes outputs to mounted volumes
   - Generates manifest JSON in `runs/YYYYMMDD_HHMMSS/manifest.json`

## Configuration

### Memory Settings

| Job Type | Sample Size | Driver Memory | Executor Memory |
|----------|-------------|---------------|-----------------|
| HTML Extract | ≤500 files | 2g | 1g |
| HTML Extract | >500 files | 4g | 2g |
| Wiki Extract | Test (<10k pages) | 8g | 4g |
| Wiki Extract | Full dump | 12g | 6g |
| Join | Default | 6g | 3g |

### Partitioning

- **Default**: 64 partitions
- **Large files**: Auto-scales to 256 partitions for >50GB files
- **Strategy**: Even distribution across partitions, coalesced to single file on write

### Spark Config

- `spark.sql.adaptive.enabled=true` (adaptive query execution)
- `spark.sql.adaptive.coalescePartitions.enabled=true`
- `spark.sql.shuffle.partitions=64` (default, configurable)
- `spark.driver.maxResultSize=4g` (for large result sets)

# Stats

## Default Configuration

| Parameter | HTML Extract | Wiki Extract | Join |
|-----------|--------------|--------------|------|
| Partitions | 64 | 64 | 64 |
| Output Dir | `workspace/store/spark` | `workspace/store/wiki` | `workspace/store/join` |

## Limits & Thresholds

| Limit | Value | Description |
|-------|-------|-------------|
| Max page buffer | 50,000 lines | Pages exceeding skipped |
| Max infobox fields | 20 per page | Infobox extraction limit |
| Abstract length | 1,000 chars | Abstract truncation |
| Large file threshold | 50GB | Triggers partition scaling (64→256) |
| Small sample | ≤500 files | Triggers lower memory (2g/1g) |
| Full Wikipedia dump | ~7M pages | Main namespace articles |

## Performance Benchmarks

| Job | Dataset Size | Duration | Driver | Executor | Partitions |
|-----|--------------|----------|--------|----------|-----------|
| HTML Extract | 500 files | ~10s | 2g | 1g | 32 |
| HTML Extract | ~28K files (~10GB) | 5-10 min | 6-8g | 4g | 256 |
| Wiki Extract | 50-100 pages | ~20-25s | 4g | 2g | 8 |
| Wiki Extract | 1,000 pages | 3-5 min | 4g | 2g | 32 |
| Wiki Extract | Full dump (~7M) | 1.5-3h | 12-16g | 6-8g | 512-1024 |

## Python vs Spark Extractor Performance Comparison

**Test Environment**: macOS Darwin 24.6.0, Docker on Apple Silicon

### Results Summary

| Sample Size | Python Extractor | Spark Extractor (Total) | Spark Job Time | Entities Extracted | Speedup    |
|-------------|------------------|-------------------------|----------------|-------------------|------------|
| 10 files    | 0.97s           | 11.82s                  | 4.13s          | 3,530             | -1118% ❌  |
| 100 files   | 4.85s           | 14.56s                  | 7.60s          | 36,370            | -200% ❌   |
| 500 files   | 22.75s          | 19.84s                  | 12.37s         | 197,284           | +13% ✅    |
| 1000 files  | 44.35s          | 26.17s                  | 18.93s         | 395,847           | +41% ✅    |

### Key Insights

**1. Spark Overhead (~7-8 seconds)**
- Docker container startup
- `pip install` dependencies
- Spark context initialization
- This fixed overhead makes Python faster for small datasets

**2. Crossover Point: ~400-500 files**
- Python: Linear scaling (~0.045s per file)
- Spark: Better parallelization, slower startup
- At 500 files: Spark becomes 13% faster (19.84s vs 22.75s)

**3. Validated Scalability**
- **1000 files** (measured): Python 44.35s, Spark 26.17s (41% faster) ✅
  - Prediction accuracy: 98% for Python, 96% for Spark
- **5000 files** (projected): Python ~220s, Spark ~55s (75% faster)
- **28000 files** (projected): Python ~1240s (21min), Spark ~300-600s (5-10min per existing benchmarks)

**4. Data Correctness**
- Both extractors produce identical entity counts at all sample sizes
- Validates that Spark implementation matches Python behavior

### Recommendation

- **< 500 files**: Use Python extractor (`python -m extractor`)
  - Avoids 7-8s Spark overhead
  - Simpler setup (no Docker required)

- **500-1000 files**: Spark begins to show advantage
  - 500 files: 13% faster
  - 1000 files: 41% faster

- **≥ 1000 files**: Use Spark extractor (`bin/spark_extract`)
  - Significantly faster (41%+ time savings)
  - Gap widens with larger datasets

- **Production datasets (10K+ files)**: Spark is essential
  - Python would take ~7-21 minutes for 10K-28K files
  - Spark completes in 5-10 minutes