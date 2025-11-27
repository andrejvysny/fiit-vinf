# Codebase Overview

## Executive Summary

This repository implements a three-stage data pipeline for analyzing public GitHub content:

1. **Crawler** - Policy-aware web crawler that fetches HTML from GitHub repositories
2. **Extractor** - Text and entity extraction from HTML (available in both single-process Python and distributed PySpark implementations)
3. **Indexer** - Inverted index builder with TF-IDF search capabilities

The system is designed with loose coupling through a shared `workspace/` directory, allowing each stage to be run independently or in sequence. The extractor stage has been enhanced with a PySpark implementation that runs in Docker containers for parallel processing of large datasets.

---

## Project Structure

```
vinf/
├── crawler/              # Web crawler module
│   ├── __main__.py      # CLI entry point
│   ├── service.py       # Main crawler service orchestrator
│   ├── crawl_frontier.py # URL queue management
│   ├── crawl_policy.py  # Robots.txt and scope enforcement
│   ├── unified_fetcher.py # HTTP client wrapper
│   ├── extractor.py     # Link extraction from HTML
│   ├── metadata_writer.py # Crawl metadata logging
│   └── url_tools.py     # URL canonicalization utilities
│
├── extractor/            # Single-process extractor (legacy)
│   ├── __main__.py      # CLI entry point
│   ├── pipeline.py      # Extraction pipeline coordinator
│   ├── html_clean.py    # HTML to text conversion
│   ├── entity_extractors.py # Regex-based entity extraction
│   ├── regexes.py       # Entity extraction patterns
│   ├── io_utils.py      # File discovery and I/O helpers
│   └── outputs.py       # Text and entity file writers
│
├── spark/                # PySpark extractor implementation
│   ├── main.py          # Legacy Spark entry point (simpler implementation)
│   ├── jobs/
│   │   └── html_extractor.py # Production Spark job (uses RDDs)
│   ├── lib/
│   │   ├── io.py        # TSV/NDJSON read/write helpers
│   │   ├── utils.py     # Manifest and logging utilities
│   │   ├── regexes.py   # Spark-compatible regex patterns
│   │   └── tokenize.py  # Tokenization utilities
│   ├── docker-compose.yml # Standalone Spark cluster (master + worker)
│   └── requirements.txt # Spark-specific dependencies
│
├── indexer/              # Inverted index builder
│   ├── build.py         # Index construction CLI
│   ├── query.py         # Search query CLI
│   ├── search.py        # TF-IDF search engine
│   ├── ingest.py        # Document ingestion and vocabulary building
│   ├── tokenize.py      # Text tokenization
│   ├── idf.py           # IDF calculation strategies
│   ├── store.py         # Index serialization (JSONL)
│   └── compare.py       # IDF method comparison tool
│
├── bin/                  # Executable scripts
│   └── spark_extract    # Spark extraction wrapper script
│
├── tests/                # Unit tests
│   ├── test_regexes.py  # Entity extraction pattern tests
│   ├── test_link_extractor.py # Link extraction tests
│   └── test_spark_extractor.py # Spark extractor tests
│
├── tools/                # Helper scripts
│   └── crawl_stats.py   # Crawl statistics analyzer
│
├── workspace/            # Runtime artifacts (not committed)
│   ├── store/
│   │   ├── html/        # HTML snapshots (SHA256 filenames)
│   │   ├── text/        # Extracted plain text files
│   │   ├── entities/    # Entity TSV files
│   │   └── index/      # Inverted index artifacts
│   ├── state/           # Crawler state (frontier, fetched URLs)
│   ├── metadata/        # Crawl metadata JSONL
│   └── logs/            # Application logs
│
├── config.yml           # Unified configuration file
├── config_loader.py     # YAML configuration loader
├── requirements.txt     # Python dependencies
├── docker-compose.spark.yml # Docker Compose for Spark extraction
└── README.md            # User-facing documentation
```

---

## Architecture Overview

### Data Flow Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│ Stage 1: Crawler                                                │
│ python -m crawler --config config.yml                          │
│                                                                 │
│ Input:  Seed URLs from config.yml                              │
│ Output: workspace/store/html/**/*.html                         │
│         workspace/metadata/crawl_metadata.jsonl                │
│         workspace/state/frontier.jsonl                         │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 2: Extractor (Two Implementations)                        │
│                                                                 │
│ Option A: Single-process Python                                │
│   python -m extractor --config config.yml                      │
│                                                                 │
│ Option B: Distributed PySpark (Docker)                          │
│   bin/spark_extract --config config.yml                        │
│                                                                 │
│ Input:  workspace/store/html/**/*.html                         │
│ Output: workspace/store/text/**/*.txt                          │
│         workspace/store/entities/entities.tsv                  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 3: Indexer                                                │
│ python -m indexer.build --config config.yml                    │
│                                                                 │
│ Input:  workspace/store/text/**/*.txt                           │
│ Output: workspace/store/index/*/docs.jsonl                    │
│         workspace/store/index/*/postings.jsonl                 │
│         workspace/store/index/*/manifest.json                  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 4: Query                                                  │
│ python -m indexer.query --query "search terms"                  │
│                                                                 │
│ Input:  workspace/store/index/*/ (from build)                   │
│ Output: Ranked search results with TF-IDF scores               │
└─────────────────────────────────────────────────────────────────┘
```

### Module Coupling

- **Loose coupling**: Each stage reads from and writes to the `workspace/` directory
- **Resumable**: Stages can be run independently; crawler maintains state for resumption
- **Deterministic**: Document IDs derived from HTML filenames (SHA256 hashes) ensure consistency
- **Config-driven**: All stages read from unified `config.yml` with CLI overrides

---

## Module Details

### 1. Crawler Module (`crawler/`)

**Purpose**: Policy-aware web crawler that fetches HTML from GitHub repositories while respecting robots.txt, rate limits, and scope constraints.

#### Key Components

- **`CrawlerScraperService`** (`service.py`): Main orchestrator that:
  - Manages async HTTP fetching with rate limiting
  - Enforces crawl policies (depth, per-repo limits, scope rules)
  - Stores HTML snapshots with SHA256-based filenames
  - Logs crawl metadata (status codes, timing, referrers)
  - Maintains frontier state for resumable crawls

- **`CrawlFrontier`** (`crawl_frontier.py`): File-backed URL queue:
  - Append-only `frontier.jsonl` log
  - Indexed lookup for deduplication
  - Periodic persistence to survive restarts

- **`CrawlPolicy`** (`crawl_policy.py`): Scope and robots.txt enforcement:
  - Robots.txt fetching and caching (TTL-based)
  - Host allow/deny patterns
  - URL pattern matching (regex-based)
  - Depth limits and per-repository quotas

- **`UnifiedFetcher`** (`unified_fetcher.py`): HTTP client wrapper:
  - User-agent rotation
  - Retry logic with exponential backoff
  - Timeout handling
  - Content-type validation

- **`LinkExtractor`** (`extractor.py`): HTML link extraction:
  - Parses HTML to find `<a href>` tags
  - Canonicalizes URLs
  - Filters by scope rules

#### Configuration (`config.yml` → `crawler` section)

- **Seeds**: Initial URLs to crawl
- **User agents**: Rotating user-agent strings
- **Robots**: robots.txt user-agent and cache TTL
- **Scope**: Allowed/denied hosts, URL patterns, content types
- **Limits**: Concurrency, rate limits, timeouts, retries
- **Caps**: Max depth, per-repo page/issue/PR limits
- **Sleep**: Per-request delays and batch pauses
- **Storage**: Paths for frontier, metadata, HTML store

#### Outputs

- `workspace/store/html/<sha256>.html` - Content-addressed HTML snapshots
- `workspace/state/frontier.jsonl` - Append-only URL queue log
- `workspace/state/fetched_urls.txt` - Deduplication ledger
- `workspace/metadata/crawl_metadata.jsonl` - Fetch outcomes (JSONL)
- `workspace/state/service_stats.json` - Runtime counters
- `workspace/logs/crawler.log` - Application logs

#### Operational Features

- **Resumable**: Frontier state persists across restarts
- **Rate limiting**: Configurable requests/second with batch pauses
- **Robots compliance**: Caches robots.txt responses with TTL
- **Scope enforcement**: Pre-fetch validation prevents crawl explosion
- **Storage tracking**: Monitors HTML file count and storage usage

---

### 2. Extractor Module (`extractor/`)

**Purpose**: Transforms stored HTML into analysis-ready artifacts (plain text and structured entities).

#### Two Implementations

**A. Single-Process Python (`extractor/`)**

- Sequential processing of HTML files
- Suitable for small-to-medium datasets
- Simple debugging and development

**B. Distributed PySpark (`spark/`)**

- Parallel processing across Spark partitions
- Suitable for large datasets (28K+ files)
- Runs in Docker containers
- **Detailed below in Spark section**

#### Key Components (Python Implementation)

- **`ExtractorPipeline`** (`pipeline.py`): Orchestrates extraction workflow
- **`HtmlFileProcessor`** (`entity_main.py`): Processes individual HTML files
- **`html_clean.html_to_text()`**: Converts HTML to plain text (preserves structure)
- **`entity_extractors.extract_all_entities()`**: Regex-based entity extraction:
  - Star counts (`STAR_COUNT`)
  - Fork counts (`FORK_COUNT`)
  - Language statistics (`LANG_STATS`)
  - README detection (`README`)
  - License detection (`LICENSE`)
  - Topics (`TOPICS`)
  - URLs (`URL`)
  - Email addresses (`EMAIL`)

#### Entity Extraction Patterns (`regexes.py`)

- GitHub-specific patterns for repository metadata
- Bounded regex patterns to avoid catastrophic backtracking
- Offset tracking for entity positions in source HTML

#### Outputs

- `workspace/store/text/**/*.txt` - Plain text files (mirrors HTML directory structure)
- `workspace/store/entities/entities.tsv` - Tab-separated entities:
  - Columns: `doc_id`, `type`, `value`, `offsets_json`
  - Streaming write (safe for large runs)

#### Configuration (`config.yml` → `extractor` section)

- `input_root`: HTML input directory
- `outputs.text`: Text output directory
- `outputs.entities`: Entity TSV file path
- `enable_text`: Toggle text extraction
- `enable_entities`: Toggle entity extraction
- `force`: Overwrite existing outputs
- `dry_run`: List files without processing

---

### 3. Indexer Module (`indexer/`)

**Purpose**: Builds and queries an inverted index over extracted text with TF-IDF scoring.

#### Key Components

- **`IndexBuilder`** (`build.py`): Constructs inverted index:
  - **Streaming architecture**: Processes documents in chunks (default 1000)
  - **Chunked processing**: Writes partial postings per chunk, then merges
  - **K-way merge**: Merges partial postings using heap-based algorithm
  - **Multiple IDF methods**: Computes classic, smoothed, probabilistic, and max IDF

- **`DocumentRecord`** (`ingest.py`): Document metadata:
  - `doc_id`: Document identifier (from filename)
  - `path`: File path
  - `title`: Extracted title (if available)
  - `length`: Character count
  - `token_count`: Optional tiktoken count

- **`build_vocabulary()`** (`ingest.py`): Term frequency aggregation:
  - Tokenizes documents
  - Builds term → document frequency mapping
  - Tracks term positions for phrase queries

- **`IDFCalculator`** (`idf.py`): Inverse document frequency strategies:
  - **Classic**: `log((N + 1) / (df + 1)) + 1`
  - **Smoothed**: `log(1 + N / df)`
  - **Probabilistic**: `log((N - df) / df)`
  - **Max**: `log(1 + max_df / df)` where `max_df` is max document frequency

- **`SearchEngine`** (`search.py`): In-memory query engine:
  - TF-IDF scoring
  - Top-K result ranking
  - Supports multiple IDF methods

- **`query.py`**: CLI wrapper for search functionality

#### Index Artifacts

- **`docs.jsonl`**: Document metadata (one JSON object per line)
- **`postings.jsonl`**: Inverted index (term → postings list):
  - Each line: `{"term": "...", "df": N, "idf": {...}, "postings": [...]}`
  - Postings: `[{"doc_id": N, "tf": M}, ...]`
- **`terms.idx`**: Term index for fast lookups:
  - Maps term → byte offset in `postings.jsonl`
- **`manifest.json`**: Index metadata:
  - Total documents, vocabulary size
  - Available IDF methods
  - Build timestamp

#### Streaming Index Construction

The indexer uses a chunked streaming approach to avoid loading all documents into memory:

1. **Chunk processing**: Documents processed in batches (default 1000)
2. **Partial postings**: Each chunk writes a partial postings file
3. **K-way merge**: Partial files merged using a heap-based algorithm
4. **Term aggregation**: Merges postings for the same term across chunks
5. **IDF computation**: Calculates IDF values after merge (requires total document count)

#### Configuration (`config.yml` → `indexer` section)

- `default_idf`: Default IDF method for queries
- `build.input_dir`: Text input directory
- `build.output_dir`: Index output directory
- `build.use_tokens`: Include tiktoken counts
- `build.token_model`: Tokenizer model (e.g., `cl100k_base`)
- `query.index_dir`: Index directory for queries
- `query.top_k`: Number of results to return
- `query.idf_method`: IDF method to use (`manifest` uses index default)

---

## Spark Implementation Details

### Overview

The Spark extractor (`spark/`) provides a distributed, parallel implementation of the HTML extraction pipeline. It runs inside Docker containers using the official Apache Spark image, allowing the host machine to only need Python and Docker.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Docker Container (apache/spark-py:latest)                   │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Spark Driver (local[*] mode)                         │  │
│  │ - Discovers HTML files                               │  │
│  │ - Creates RDD partitions                              │  │
│  │ - Coordinates worker tasks                           │  │
│  └──────────────────────────────────────────────────────┘  │
│                        │                                     │
│                        ▼                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Spark Executors (parallel partitions)                │  │
│  │                                                       │  │
│  │ Partition 1: [file1.html, file2.html, ...]          │  │
│  │   ├─ html_to_text()                                  │  │
│  │   └─ extract_all_entities()                         │  │
│  │                                                       │  │
│  │ Partition 2: [fileN.html, fileN+1.html, ...]        │  │
│  │   ├─ html_to_text()                                  │  │
│  │   └─ extract_all_entities()                          │  │
│  │                                                       │  │
│  │ ... (N partitions)                                    │  │
│  └──────────────────────────────────────────────────────┘  │
│                        │                                     │
│                        ▼                                     │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Aggregation & Output                                 │  │
│  │ - Combine statistics across partitions               │  │
│  │ - Write text files (via extractor.outputs)          │  │
│  │ - Write entities TSV (via spark.lib.io)             │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Two Spark Implementations

#### 1. `spark/main.py` (Simpler, Legacy)

- **Purpose**: Simpler implementation for basic use cases
- **Approach**: Uses `mapPartitionsWithIndex` for processing
- **Output**: Writes to `workspace/store/spark/` directory
- **Features**:
  - File discovery on driver
  - Direct RDD processing
  - Manifest generation with code hashing

#### 2. `spark/jobs/html_extractor.py` (Production, Recommended)

- **Purpose**: Production-ready implementation with better integration
- **Approach**: Uses RDDs with broadcast configuration
- **Output**: Writes to same locations as Python extractor (`workspace/store/text/`, `workspace/store/entities/`)
- **Features**:
  - Reuses `extractor` module components directly
  - Broadcast configuration to workers
  - Statistics aggregation
  - TSV writing via `spark.lib.io.write_tsv`

### Key Components

#### `spark/jobs/html_extractor.py`

**Entry Point**: `main()` function
- Parses CLI arguments
- Loads configuration via `ExtractorConfig`
- Discovers HTML files using `HtmlFileDiscovery`
- Builds Spark session
- Runs Spark job
- Aggregates and reports statistics

**Core Processing**: `_process_partition()`
- Runs on each Spark partition (worker)
- Processes batch of HTML files:
  1. Reads HTML content
  2. Calls `html_clean.html_to_text()` (reuses extractor module)
  3. Calls `entity_extractors.extract_all_entities()` (reuses extractor module)
  4. Writes text files via `extractor.outputs.write_text()`
  5. Collects entities for TSV output
- Returns statistics tuple and entity rows

**Statistics Aggregation**: `run_spark_job()`
- Creates RDD from file paths
- Maps partitions through `_process_partition()`
- Aggregates statistics using `fold()` operation
- Writes entities TSV using `spark.lib.io.write_tsv()`

**Entity TSV Writing**: `_write_entities()`
- Converts entity RDD to DataFrame
- Sanitizes TSV fields (removes tabs, newlines)
- Uses `spark.lib.io.write_tsv()` to write single TSV file

#### `spark/lib/io.py`

**Purpose**: Spark-compatible I/O helpers

**`write_tsv()`**: Writes DataFrame to single TSV file
- Uses Spark's CSV writer with tab separator
- Coalesces to 1 partition for single output file
- Writes to temporary directory, then moves single `part-*` file to target
- Handles header row option

**`read_tsv()`**: Reads TSV into DataFrame
- Uses Spark's CSV reader with tab separator
- Handles UTF-8 encoding

**`write_ndjson()`**: Writes DataFrame to newline-delimited JSON
- Coalesces to 1 partition
- Uses Spark's JSON writer

**`read_ndjson()`**: Reads NDJSON into DataFrame

#### `spark/lib/utils.py`

**Purpose**: Utility functions for manifests and logging

- **`sha1_hexdigest()`**: Compute file checksums
- **`write_manifest()`**: Atomically write JSON manifests
- **`StructuredLogger`**: JSONL logger for pipeline telemetry

### Docker Configuration

#### `docker-compose.spark.yml` (Primary, Single Container)

**Purpose**: Simple single-container deployment for local Spark execution

**Service**: `spark`
- **Image**: `apache/spark-py:latest` (official Apache Spark with Python)
- **Platform**: `linux/amd64` (ensures compatibility on macOS ARM64 via emulation)
- **Working Directory**: `/opt/app`
- **Environment Variables**:
  - `SPARK_NO_DAEMONIZE=1`: Keep Spark in foreground
  - `SPARK_DRIVER_MEMORY`: Driver memory (default 4g, configurable)
  - `SPARK_EXECUTOR_MEMORY`: Executor memory (default 2g, configurable)
  - `SPARK_LOCAL_DIRS=/tmp/spark`: Spark local directories
  - `PYTHONPATH=/opt/app`: Python path for imports
  - `PYSPARK_PYTHON=python3`: Python interpreter for PySpark
  - `HOME=/tmp`: Home directory (required by some Spark operations)

**Volumes** (Bind Mounts):
- `./workspace/store/html:/opt/app/workspace/store/html:ro` - Read-only HTML input
- `./workspace/store/spark:/opt/app/workspace/store/spark` - Spark output directory
- `./spark:/opt/app/spark:ro` - Spark job code (read-only)
- `./extractor:/opt/app/extractor:ro` - Extractor module (read-only)
- `./config.yml:/opt/app/config.yml:ro` - Configuration (read-only)
- `./requirements.txt:/opt/app/requirements.txt:ro` - Dependencies (read-only)
- `./config_loader.py:/opt/app/config_loader.py:ro` - Config loader (read-only)
- `./runs:/opt/app/runs` - Run manifests

**Command**:
```bash
bash -c "
  pip install --no-cache-dir --disable-pip-version-check -q -r /opt/app/requirements.txt &&
  /opt/spark/bin/spark-submit --master local[*] --driver-memory 4g /opt/app/spark/main.py --config /opt/app/config.yml
"
```

**User**: `0:0` (root) - Avoids permission issues with file writes

**Network**: `spark-net` (bridge driver)

#### `spark/docker-compose.yml` (Standalone Cluster)

**Purpose**: Multi-container Spark cluster (master + worker) for distributed execution

**Services**:

1. **`spark-master`**:
   - Runs Spark standalone master
   - Ports: `7077` (master port), `8080` (web UI)
   - Command: `org.apache.spark.deploy.master.Master`
   - Host: `spark-master` (internal DNS)

2. **`spark-worker`**:
   - Runs Spark worker
   - Connects to `spark://spark-master:7077`
   - Resources: 4 cores, 4GB memory
   - Ports: `8081` (worker web UI)
   - Depends on: `spark-master`

3. **`spark-job`** (Profile: `job`):
   - Interactive job container
   - Entrypoint: `/bin/bash -lc`
   - Command: `sleep infinity` (keeps container running)
   - Used for manual `spark-submit` commands

**Network**: `spark-net` (bridge driver)

**Usage**: For distributed execution across multiple workers (not typically used for local development)

### Execution Flow

#### Via `bin/spark_extract` Script

1. **Script Location**: `bin/spark_extract`
2. **Checks**: Verifies Docker availability
3. **Local Fallback**: `--local` flag runs Python extractor instead
4. **Argument Parsing**: Extracts Spark-specific arguments (`--sample`, `--partitions`, `--force`, `--dry-run`)
5. **Memory Configuration**: Sets `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY` based on sample size
6. **Docker Compose Execution**: Runs `docker compose -f docker-compose.spark.yml run --rm spark`
7. **Spark Submit**: Executes `spark-submit` with appropriate arguments

#### Direct Docker Compose

```bash
docker compose -f docker-compose.spark.yml run --rm \
  -e SPARK_DRIVER_MEMORY=4g \
  -e SPARK_EXECUTOR_MEMORY=2g \
  spark bash -c "pip install -r /opt/app/requirements.txt && \
  /opt/spark/bin/spark-submit --master local[*] \
  --driver-memory 4g /opt/app/spark/jobs/html_extractor.py \
  --config /opt/app/config.yml --partitions 64"
```

### Spark Configuration

#### Memory Settings

- **Driver Memory**: Default 4GB, configurable via `SPARK_DRIVER_MEMORY`
- **Executor Memory**: Default 2GB, configurable via `SPARK_EXECUTOR_MEMORY`
- **Auto-tuning**: `bin/spark_extract` adjusts memory based on sample size:
  - Small samples (≤500 files): 2GB driver, 1GB executor
  - Large samples: 4GB driver, 2GB executor

#### Partitioning

- **Default Partitions**: 64 (configurable via `--partitions`)
- **Partition Strategy**: Files distributed evenly across partitions
- **Skew Mitigation**: Statistics track per-partition record counts

#### Execution Mode

- **Mode**: `local[*]` (uses all available CPU cores on host)
- **Alternative**: Can connect to `spark://spark-master:7077` for cluster mode

### Integration with Extractor Module

The Spark implementation **reuses** the existing `extractor` module components:

- **`html_clean.html_to_text()`**: Direct import and call
- **`entity_extractors.extract_all_entities()`**: Direct import and call
- **`extractor.outputs.write_text()`**: Used for text file writing
- **`extractor.io_utils.HtmlFileDiscovery`**: Used for file discovery

This ensures **100% output parity** between Python and Spark implementations.

### Output Locations

The Spark extractor writes to the same locations as the Python extractor:

- **Text Files**: `workspace/store/text/**/*.txt` (via `extractor.outputs.write_text()`)
- **Entities TSV**: `workspace/store/entities/entities.tsv` (via `spark.lib.io.write_tsv()`)

This allows the indexer to work with outputs from either implementation.

### Performance Characteristics

- **Parallelization**: Processes multiple HTML files simultaneously across Spark partitions
- **Scalability**: Handles large datasets (28K+ files) efficiently
- **Resource Usage**: Configurable memory and partition count
- **Fault Tolerance**: Spark's built-in retry mechanisms for failed tasks

### Limitations

- **Local Mode Only**: Current implementation uses `local[*]` mode (single machine)
- **Docker Required**: Requires Docker for execution (not pure Python)
- **Memory Intensive**: Large datasets may require significant memory allocation

---

## Configuration System

### Unified Configuration (`config.yml`)

All modules read from a single YAML configuration file with the following structure:

```yaml
workspace: "./workspace"  # Base workspace directory

crawler:
  run_id: "unified-2025-01-12"
  seeds: [...]  # Seed URLs
  user_agents: [...]  # User agent strings
  robots: {...}  # Robots.txt settings
  scope: {...}  # URL scope rules
  limits: {...}  # Rate limits, timeouts
  caps: {...}  # Depth and per-repo limits
  sleep: {...}  # Delay settings
  storage: {...}  # File paths
  logs: {...}  # Logging configuration

extractor:
  input_root: "store/html"
  outputs:
    text: "store/text"
    entities: "store/entities/entities.tsv"
  enable_text: true
  enable_entities: true
  force: false
  dry_run: false

indexer:
  default_idf: "classic"
  build:
    input_dir: "store/text"
    output_dir: "store/index/default"
    limit: null
    use_tokens: true
    token_model: "cl100k_base"
  query:
    index_dir: "store/index/default"
    top_k: 10
    idf_method: "manifest"
    show_path: false
```

### Configuration Loading

- **`config_loader.py`**: YAML loader with error handling
- **Module-specific parsers**: Each module has a `config.py` that parses relevant sections
- **CLI Overrides**: All modules support `--config` flag and individual option overrides

### Path Resolution

- **Relative paths**: Resolved relative to `workspace` directory
- **Absolute paths**: Used as-is
- **Default resolution**: Module-specific defaults if not specified

---

## Testing

### Test Structure

Tests live in `tests/` directory and use Python's `unittest` framework:

- **`test_regexes.py`**: Tests entity extraction regex patterns
- **`test_link_extractor.py`**: Tests HTML link extraction
- **`test_spark_extractor.py`**: Tests Spark extractor functionality

### Running Tests

```bash
python -m unittest discover tests
```

### Test Patterns

- **Fixtures**: Deterministic HTML/text samples for reproducible testing
- **Regression tests**: Ensure extraction patterns don't break
- **Integration tests**: Verify end-to-end pipeline functionality

---

## Workspace Layout

The `workspace/` directory (not committed to git) contains all runtime artifacts:

```
workspace/
├── store/
│   ├── html/              # HTML snapshots (SHA256 filenames)
│   │   └── <sha256>.html
│   ├── text/              # Plain text files (mirrors HTML structure)
│   │   └── <doc_id>.txt
│   ├── entities/          # Entity TSV files
│   │   └── entities.tsv
│   └── index/             # Inverted index artifacts
│       └── default/
│           ├── docs.jsonl
│           ├── postings.jsonl
│           ├── terms.idx
│           └── manifest.json
│
├── state/                 # Crawler state
│   ├── frontier.jsonl     # URL queue
│   ├── fetched_urls.txt  # Deduplication ledger
│   ├── robots_cache.jsonl # Cached robots.txt
│   └── service_stats.json # Runtime statistics
│
├── metadata/              # Crawl metadata
│   └── crawl_metadata.jsonl
│
└── logs/                  # Application logs
    ├── crawler.log
    └── extractor.log
```

### File Naming Conventions

- **HTML files**: SHA256 hash of content (ensures deduplication)
- **Text files**: Document ID (stem of HTML filename)
- **Index files**: Standard names (`docs.jsonl`, `postings.jsonl`, etc.)

---

## Dependencies

### Core Dependencies (`requirements.txt`)

- **`httpx[http2]`**: HTTP client with HTTP/2 support (crawler)
- **`pyyaml`**: YAML configuration parsing
- **`tiktoken`**: Token counting for indexer (optional)

### Spark Dependencies

- **PySpark**: Included in `apache/spark-py` Docker image
- **Project modules**: Mounted as read-only volumes in Docker

### Development Dependencies

- **`unittest`**: Built-in Python testing framework
- **Docker**: Required for Spark execution

---

## Execution Examples

### Full Pipeline

```bash
# 1. Crawl GitHub repositories
python -m crawler --config config.yml

# 2. Extract text and entities (Spark)
bin/spark_extract --config config.yml

# 3. Build inverted index
python -m indexer.build --config config.yml

# 4. Query the index
python -m indexer.query --config config.yml --query "async crawler"
```

### Individual Stages

```bash
# Extract with sample size
bin/spark_extract --config config.yml --sample 100

# Extract with custom partitions
bin/spark_extract --config config.yml --partitions 128

# Force reprocessing
bin/spark_extract --config config.yml --force

# Dry run (list files only)
bin/spark_extract --config config.yml --dry-run

# Fallback to Python extractor
bin/spark_extract --local --config config.yml
```

---

## Key Design Decisions

1. **Content-Addressed Storage**: HTML files named by SHA256 hash ensures deduplication
2. **Loose Coupling**: Stages communicate via filesystem, enabling independent execution
3. **Deterministic IDs**: Document IDs derived from filenames ensure consistency
4. **Streaming Indexing**: Chunked processing avoids loading entire corpus into memory
5. **Multiple IDF Methods**: Index stores multiple IDF strategies for comparison
6. **Spark Reuse**: Spark implementation reuses extractor module for parity
7. **Docker Isolation**: Spark runs in containers, host only needs Python + Docker
8. **Config-Driven**: Unified configuration with CLI overrides for flexibility

---

## Future Enhancements

Potential improvements (not currently implemented):

- **Distributed Spark Cluster**: Multi-node Spark cluster for very large datasets
- **Incremental Indexing**: Update index with new documents without full rebuild
- **Query Optimization**: Index-based query optimization for faster searches
- **Entity Indexing**: Index entities separately for entity-based queries
- **Compression**: Compress HTML/text files to reduce storage
- **Monitoring**: Real-time monitoring dashboard for crawl/extraction progress

---

## Summary

This codebase implements a complete pipeline for crawling, extracting, and indexing GitHub content. The architecture emphasizes:

- **Modularity**: Three independent stages with clear interfaces
- **Scalability**: Spark implementation for parallel processing
- **Reproducibility**: Deterministic outputs and comprehensive logging
- **Flexibility**: Config-driven with CLI overrides
- **Maintainability**: Clean separation of concerns and comprehensive documentation

The Spark implementation provides a production-ready solution for processing large datasets while maintaining 100% output parity with the single-process Python implementation.

