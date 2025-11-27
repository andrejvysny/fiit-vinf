# GitHub-Wikipedia Data Pipeline: Complete Codebase Description

## Project Overview

This repository implements a **multi-stage data pipeline** for extracting, analyzing, and enriching data from public GitHub repositories with knowledge from Wikipedia. The system is designed for large-scale information retrieval research, processing 28,000+ HTML files from GitHub and linking extracted entities with Wikipedia's 7+ million articles.

**Primary Use Cases:**
- Crawl and archive GitHub repository pages
- Extract structured entities (topics, programming languages, licenses, star/fork counts)
- Build searchable indexes with TF-IDF and Apache Lucene
- Enrich GitHub entities with Wikipedia knowledge through entity linking

**Tech Stack:**
- **Language:** Python 3.13
- **Distributed Processing:** Apache Spark (PySpark)
- **Search Engine:** Custom TF-IDF implementation + PyLucene
- **HTTP Client:** httpx with HTTP/2 support
- **Configuration:** YAML-based central configuration

---

## System Architecture

The pipeline consists of **6 loosely-coupled stages** connected through a shared `workspace/` directory:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW DIAGRAM                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   config.yml                                                                │
│       │                                                                     │
│       ▼                                                                     │
│   ┌────────────┐     ┌───────────────┐     ┌────────────┐                  │
│   │  CRAWLER   │────►│  HTML FILES   │────►│ EXTRACTOR  │                  │
│   │ (crawler/) │     │ workspace/    │     │(extractor/)│                  │
│   └────────────┘     │ store/html/   │     └─────┬──────┘                  │
│                      └───────────────┘           │                          │
│                                                  │                          │
│                      ┌───────────────┐           │                          │
│                      │  TEXT FILES   │◄──────────┤                          │
│                      │ + ENTITIES    │           │                          │
│                      │   TSV         │           │                          │
│                      └───────┬───────┘           │                          │
│                              │                   │                          │
│                              ▼                   │                          │
│                      ┌───────────────┐           │                          │
│                      │   INDEXER     │           │                          │
│                      │  (indexer/)   │           │                          │
│                      └───────────────┘           │                          │
│                                                  │                          │
│   ┌────────────────────────────────────────────┘                           │
│   │                                                                         │
│   │   WIKIPEDIA EXTENSION PIPELINE                                          │
│   │   ════════════════════════════                                          │
│   │                                                                         │
│   │   ┌─────────────┐     ┌───────────────┐                                │
│   │   │ WIKI DUMP   │────►│ WIKI          │                                │
│   │   │ (104GB XML) │     │ EXTRACTOR     │                                │
│   │   └─────────────┘     │(wiki_extractor)│                               │
│   │                       └───────┬───────┘                                │
│   │                               │                                         │
│   │                               ▼                                         │
│   │                       ┌───────────────┐                                │
│   │                       │ WIKI TSVs     │                                │
│   │                       │ (7 files)     │                                │
│   │                       │ + TEXT/       │                                │
│   │                       └───────┬───────┘                                │
│   │                               │                                         │
│   │                               ▼                                         │
│   │   ┌───────────────┐   ┌───────────────┐                                │
│   │   │  ENTITIES     │──►│ JOIN          │                                │
│   │   │  TSV          │   │ (batch/stream)│                                │
│   │   └───────────────┘   └───────┬───────┘                                │
│   │                               │                                         │
│   │                               ▼                                         │
│   │                       ┌───────────────┐                                │
│   │                       │ ENRICHED      │                                │
│   │                       │ ENTITIES      │                                │
│   │                       │ + LUCENE IDX  │                                │
│   │                       └───────────────┘                                │
│   │                                                                         │
└───┴─────────────────────────────────────────────────────────────────────────┘
```

---

## Stage 1: Web Crawler (`crawler/`)

### Purpose
Policy-aware web crawler that fetches and archives HTML content from GitHub repositories while respecting robots.txt, rate limits, and crawl scope rules.

### Key Components

| File | Class/Module | Responsibility |
|------|--------------|----------------|
| `service.py` | `CrawlerScraperService` | Main orchestrator - coordinates URL scheduling, fetching, storage |
| `crawl_frontier.py` | `CrawlFrontier` | File-backed URL queue with O(1) deduplication via index directories |
| `crawl_policy.py` | `CrawlPolicy` | URL validation against scope rules, robots.txt compliance |
| `unified_fetcher.py` | `UnifiedFetcher` | HTTP client with retry logic, exponential backoff, compression |
| `extractor.py` | `LinkExtractor` | Regex-based link extraction from HTML |
| `metadata_writer.py` | `UnifiedMetadataWriter` | JSONL logging of fetch metadata |

### Features
- **Asynchronous I/O:** Uses `asyncio` for concurrent HTTP fetches
- **Rate Limiting:** Configurable requests/second and per-host concurrency limits
- **Robots.txt Caching:** JSONL-cached policy with TTL control
- **Content Addressing:** HTML files stored with SHA256-based filenames
- **Resumable State:** Frontier and dedup ledger persist across restarts
- **Scope Control:** Allow/deny patterns, subdomain blacklisting, per-repo caps

### Configuration (config.yml)
```yaml
crawler:
  seeds:
    - https://github.com/python/cpython
    - https://github.com/torvalds/linux
  scope:
    allowed_hosts: ["github.com"]
    deny_patterns: ["/blob/", "/tree/", "/commits/"]
  limits:
    global_concurrency: 4
    per_host_concurrency: 2
    req_per_sec: 1.0
  caps:
    per_repo_max_pages: 30
    max_depth: 4
```

### Command
```bash
python -m crawler --config config.yml
```

### Outputs
- `workspace/store/html/{SHA256}.html` - Archived HTML content
- `workspace/state/frontier.jsonl` - URL queue
- `workspace/state/fetched_urls.txt` - Deduplication ledger
- `workspace/metadata/crawl_metadata.jsonl` - Fetch metadata (status, timing, depth)

---

## Stage 2: Entity Extractor (`extractor/` + `spark/jobs/html_extractor.py`)

### Purpose
Transforms HTML files into analysis-ready artifacts: clean text and structured entity annotations.

### Dual Implementation
1. **Single-process Python** (`extractor/`) - For small datasets or debugging
2. **Distributed Spark** (`spark/jobs/html_extractor.py`) - For production (28K+ files)

### Key Components

| File | Class/Module | Responsibility |
|------|--------------|----------------|
| `pipeline.py` | `ExtractorPipeline` | Coordinates file discovery and processing |
| `entity_extractors.py` | Various functions | Entity extraction for specific types |
| `regexes.py` | `PATTERNS` dict | 60+ compiled regex patterns |
| `html_clean.py` | Cleaning functions | HTML boilerplate removal |

### Extracted Entity Types

| Entity Type | Description | Example Values |
|-------------|-------------|----------------|
| `TOPICS` | Repository topics/tags | `python`, `machine-learning`, `docker` |
| `LANG_STATS` | Programming languages with percentages | `Python 45.2%`, `JavaScript 30.1%` |
| `LICENSE` | Software license | `MIT`, `Apache-2.0`, `GPL-3.0` |
| `STAR_COUNT` | GitHub stars | `52400`, `182k` |
| `FORK_COUNT` | GitHub forks | `8200`, `41.2k` |
| `URL` | Extracted links | `https://docs.python.org` |
| `README` | Keywords from README | Various extracted terms |

### Regex Patterns (examples)
```python
# Star count extraction
STARS_PATTERNS = [
    re.compile(r'<span[^>]*id="repo-stars-counter-star"[^>]*title="([0-9,]+)"'),
    re.compile(r'aria-label="([0-9,]+)\s*star'),
]

# Programming language detection
LANG_STATS_PATTERN = re.compile(
    r'<span[^>]*itemprop="programmingLanguage"[^>]*>([^<]+)</span>'
)
```

### Command
```bash
# Spark (recommended for large datasets)
bin/spark_extract --config config.yml --partitions 256

# Single-process Python
python -m extractor --config config.yml --limit 1000
```

### Outputs
- `workspace/store/text/{doc_id}.txt` - Cleaned text content
- `workspace/store/entities/entities.tsv` - Entity annotations (doc_id, type, value, offsets_json)

### Performance
- **28K files:** 5-10 minutes with Spark (256 partitions)
- **Speedup:** 1.4x faster than single-process at 1K files

---

## Stage 3: Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

### Purpose
Extracts structured data from Wikipedia XML dumps (100GB+) without running out of memory. Uses streaming architecture with NO caching.

### Key Features
- **Streaming Processing:** `mapPartitions` with buffer limits (max 50K lines/page)
- **Auto-scaling Partitions:** 64 for small files, 256-1024 for >50GB
- **Content Deduplication:** SHA256-based text file storage
- **Title Normalization:** Lowercase, ASCII-fold, punctuation collapse

### Output TSV Files

| File | Description | Columns |
|------|-------------|---------|
| `pages.tsv` | Page metadata | page_id, title, norm_title, namespace, redirect_to |
| `aliases.tsv` | Redirect mappings (~8M entries) | alias_norm_title, canonical_norm_title |
| `categories.tsv` | Page-category relationships | page_id, category, norm_category |
| `abstract.tsv` | First paragraph text | page_id, abstract_text |
| `infobox.tsv` | Structured key-value pairs | page_id, key, value |
| `links.tsv` | Internal wiki links | page_id, link_title, norm_link_title |
| `wiki_text_metadata.tsv` | Page-to-text mapping | page_id, title, content_sha256, content_length |
| `text/` | Full article text (deduplicated) | {SHA256}.txt files |

### Regex Patterns for MediaWiki
```python
# XML field extraction
PAGE_PATTERN = re.compile(r'<page>(.*?)</page>', re.DOTALL)
TITLE_PATTERN = re.compile(r'<title>([^<]+)</title>')
TEXT_PATTERN = re.compile(r'<text[^>]*>([^<]*)</text>', re.DOTALL)

# Wikitext parsing
CATEGORY_PATTERN = re.compile(r'\[\[Category:([^\]|]+)')
INFOBOX_PATTERN = re.compile(r'\{\{Infobox\s+([^|}]+)')
```

### Command
```bash
# Test (100 pages, ~25 seconds)
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# Full extraction (~2-3 hours, requires 16-32GB RAM)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512
```

### Performance
| Dataset | Pages | Duration | Memory | Partitions |
|---------|-------|----------|--------|------------|
| Test | 100 | 20-25 sec | 4g | 8 |
| Medium | 1,000 | 3-5 min | 4g | 32 |
| Full dump | 7M | 1.5-3 hours | 12-16g | 512-1024 |

---

## Stage 4: Entity-Wikipedia Join

### 4a. Batch Join (`spark/jobs/join_html_wiki.py`)

### Purpose
Links GitHub entities with Wikipedia canonical pages through normalized title matching and alias resolution.

### Join Algorithm
1. Build canonical mapping: Wikipedia pages + aliases → normalized titles
2. Normalize entity values (lowercase, ASCII-fold, punctuation collapse)
3. Explode comma-separated TOPICS into individual rows
4. LEFT JOIN on normalized keys (preserves unmatched entities)
5. Calculate confidence scores [0.6-1.0]

### Confidence Scoring
| Factor | Score Contribution |
|--------|-------------------|
| Base score | 0.6 |
| Direct match | +0.2 |
| Alias match | +0.1 |
| Case exactness | +0.1 |
| Category relevance | +0.1 |

### Command
```bash
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join
```

### Outputs
- `html_wiki.tsv` - Full join results
- `join_stats.json` - Match statistics by entity type
- `html_wiki_agg.tsv` - Per-document aggregates

### Expected Match Rates
| Entity Type | Match Rate |
|-------------|------------|
| LANG_STATS | 60-90% |
| LICENSE | 80-95% |
| TOPICS | 30-50% |
| README | 10-30% |

---

### 4b. Streaming Join (`spark/jobs/join_html_wiki_topics.py`)

### Purpose
High-quality TOPICS-to-Wikipedia join with relevance filtering using Spark Structured Streaming.

### Key Features
- **Bounded Memory:** `maxFilesPerTrigger=16` prevents OOM
- **Multi-hop Resolution:** entities → aliases → pages → categories + abstracts
- **Relevance Filtering:** Only technology-related Wikipedia pages
- **Checkpoint-based Resumability**

### Relevance Keywords
`programming`, `software`, `computer`, `library`, `framework`, `license`

### Confidence Levels
| Level | Description |
|-------|-------------|
| `exact+cat` | Direct title match + relevant category |
| `alias+cat` | Alias resolution + relevant category |
| `exact+abs` | Direct match + abstract contains topic |
| `alias+abs` | Alias resolution + abstract contains topic |

### Command
```bash
bin/spark_join_wiki_topics \
  --entities workspace/store/spark/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/wiki/join
```

---

## Stage 5: TF-IDF Indexer (`indexer/`)

### Purpose
Builds and queries an inverted index over text documents using TF-IDF scoring with multiple IDF strategies.

### Key Components

| File | Class/Module | Responsibility |
|------|--------------|----------------|
| `build.py` | CLI entry point | Index construction |
| `search.py` | `SearchEngine` | TF-IDF query engine |
| `idf.py` | IDF functions | IDF computation strategies |
| `ingest.py` | `DocumentRecord` | Document tokenization |
| `store.py` | Serialization | JSONL-based index storage |

### IDF Methods

| Method | Formula | Use Case |
|--------|---------|----------|
| Classic | `log(N/df)` | Standard TF-IDF |
| Smoothed | `log(1 + (N-df)/df)` | Avoids zero IDF |
| Probabilistic | `log((N-df)/df)` | BM25-style weighting |
| Max | `log(N/min(df, 1))` | Maximum discrimination |

### Commands
```bash
# Build index
python3 -m indexer.build --config config.yml

# Query index
python3 -m indexer.query --query "python web scraping" --top 10

# Compare IDF methods
python3 -m indexer.compare --query "search ranking" --output reports/idf_comparison.md
```

### Output Files
- `docs.jsonl` - Document metadata (doc_id, path, title, length)
- `postings.jsonl` - Term postings with IDF values
- `manifest.json` - Index metadata and statistics

---

## Stage 6: PyLucene Indexer (`lucene_indexer/`)

### Purpose
Advanced full-text search using Apache Lucene with support for Boolean, Range, Phrase, and Fuzzy queries.

### Index Schema (20+ fields)

| Field | Type | Purpose |
|-------|------|---------|
| `doc_id` | StringField | Document identifier (SHA256) |
| `title` | TextField | Tokenized for full-text search |
| `content` | TextField | Main document text (not stored) |
| `topics` | TextField (multi) | GitHub repository topics |
| `languages` | TextField (multi) | Programming languages |
| `license` | StringField | Exact license matching |
| `star_count` | IntPoint | Range queries on popularity |
| `fork_count` | IntPoint | Range queries on forks |
| `wiki_title` | TextField (multi) | Linked Wikipedia titles |
| `wiki_categories` | TextField (multi) | Wikipedia categories |
| `wiki_abstract` | TextField | Wikipedia lead sections |
| `join_confidence` | StringField (multi) | Wiki join confidence level |

### Query Types

| Type | Description | Example |
|------|-------------|---------|
| Simple | Multi-field text search | `python web` |
| Boolean | AND/OR/NOT operators | `python AND docker` |
| Range | Numeric field filtering | `star_count:[1000 TO *]` |
| Phrase | Exact phrase matching | `"machine learning"` |
| Fuzzy | Typo-tolerant matching | `pyhton~2` |
| Combined | Multiple filters | text + topics + min_stars |

### Commands
```bash
# Build index
python -m lucene_indexer.build \
  --text-dir workspace/store/text \
  --entities workspace/store/entities/entities.tsv \
  --wiki-join workspace/store/join/html_wiki.tsv \
  --output workspace/store/lucene_index

# Search queries
python -m lucene_indexer.search --query "python web" --type simple
python -m lucene_indexer.search --query "python AND docker" --type boolean
python -m lucene_indexer.search --field star_count --min-value 1000 --type range
python -m lucene_indexer.search --query '"machine learning"' --type phrase
python -m lucene_indexer.search --query "pyhton" --type fuzzy

# Compare TF-IDF vs Lucene
python -m lucene_indexer.compare --queries "python,docker,web" --output reports/comparison.md
```

---

## Configuration System

### Central Configuration (`config.yml`)

The entire pipeline is controlled by a single YAML configuration file:

```yaml
workspace: "./workspace"  # Root for all artifacts

crawler:
  run_id: "unified-2025-01-12"
  seeds:
    - https://github.com/python/cpython
    - https://github.com/torvalds/linux
  user_agents:
    - "ResearchCrawlerBotVINF/2.0 (+mailto:xvysnya@stuba.sk)"
  scope:
    allowed_hosts: ["github.com"]
    deny_patterns: ["/blob/", "/tree/"]
  limits:
    global_concurrency: 4
    req_per_sec: 1.0
  caps:
    per_repo_max_pages: 30
    max_depth: 4

extractor:
  input_root: "store/html"
  outputs:
    text: "store/text"
    entities: "store/entities/entities.tsv"

indexer:
  default_idf: "classic"
  build:
    input_dir: "store/text"
    output_dir: "store/index/default"
  query:
    top_k: 10
    idf_method: "manifest"
```

---

## Workspace Directory Structure

```
workspace/
├── state/                          # Crawler state
│   ├── frontier.jsonl             # URL queue
│   ├── fetched_urls.txt           # Deduplication ledger
│   └── robots_cache.jsonl         # Robots.txt cache
├── metadata/
│   └── crawl_metadata.jsonl       # Fetch outcomes
├── store/
│   ├── html/                      # SHA256-named HTML files
│   │   └── {SHA256}.html
│   ├── text/                      # Extracted text
│   │   └── {doc_id}.txt
│   ├── entities/
│   │   └── entities.tsv          # Entity annotations
│   ├── index/                     # TF-IDF index
│   │   ├── docs.jsonl
│   │   ├── postings.jsonl
│   │   └── manifest.json
│   ├── lucene_index/              # PyLucene index
│   ├── wiki/                      # Wikipedia data
│   │   ├── pages.tsv
│   │   ├── aliases.tsv
│   │   ├── categories.tsv
│   │   ├── abstract.tsv
│   │   ├── infobox.tsv
│   │   ├── links.tsv
│   │   ├── wiki_text_metadata.tsv
│   │   └── text/                  # Full article text
│   │       └── {SHA256}.txt
│   └── join/                      # Entity-Wiki join results
│       ├── html_wiki.tsv
│       ├── join_stats.json
│       └── html_wiki_agg.tsv
└── logs/
    └── crawler.log
```

---

## Test Suite

### Test Files

| File | Coverage |
|------|----------|
| `tests/test_regexes.py` | HTML cleaning patterns, GitHub entity patterns |
| `tests/test_link_extractor.py` | URL extraction and joining |
| `tests/test_wiki_regexes.py` | MediaWiki XML parsing |
| `tests/test_spark_extractor.py` | Spark RDD operations |

### Commands
```bash
# Run all tests
python -m unittest discover tests

# Run specific test file
python -m unittest tests.test_regexes

# Run single test method
python -m unittest tests.test_regexes.TestRegexes.test_stars_pattern
```

---

## Dependencies

### Core Dependencies
```
httpx[http2]>=0.27.0    # HTTP client with HTTP/2
pyyaml>=6.0             # Configuration parsing
tiktoken>=0.5.0         # Token counting for OpenAI models
pyspark>=3.5.0          # Distributed processing
```

### Optional Dependencies
- **PyLucene:** Requires manual build from source (see `PYLUCENE_INSTALL.md`)
- **Docker:** For containerized Spark execution

---

## Complete Pipeline Workflow

```bash
# 1. Setup environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Crawl GitHub (hours, ongoing)
python -m crawler --config config.yml

# 3. Extract HTML entities (5-10 minutes)
export SPARK_DRIVER_MEMORY=6g
export SPARK_EXECUTOR_MEMORY=4g
bin/spark_extract --partitions 256

# 4. Extract Wikipedia data (test first!)
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# 5. Full Wikipedia extraction (2-3 hours, 16-32GB RAM)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512

# 6. Join entities with Wikipedia (15-30 minutes)
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# 7. Build TF-IDF index (minutes)
python3 -m indexer.build --config config.yml

# 8. Build Lucene index (minutes)
python -m lucene_indexer.build \
  --text-dir workspace/store/text \
  --entities workspace/store/entities/entities.tsv \
  --wiki-join workspace/store/join/html_wiki.tsv

# 9. Query results
python3 -m indexer.query --query "python web scraping" --top 10
python -m lucene_indexer.search --query "python AND docker" --type boolean
```

---

## Performance Summary

| Stage | Input Size | Output | Duration | Memory |
|-------|-----------|--------|----------|--------|
| Crawler | 10 seeds | 28K HTML, 10GB | Hours | 2GB |
| HTML Extractor | 28K files | 400K entities | 5-10 min | 6-8GB driver |
| Wiki Extractor | 104GB XML | 7M pages, 8M aliases | 1.5-3 hours | 12-16GB driver |
| Batch Join | 400K entities | 200K matches | 15-30 min | 6GB driver |
| TF-IDF Index | 28K texts | Index artifacts | Minutes | 4GB |
| Lucene Index | Text + entities | Lucene index | Minutes | 4-8GB |

---

## Key Technical Decisions

1. **Content Addressing:** SHA256 filenames prevent duplicates and enable cross-stage linking
2. **Streaming Architecture:** Wikipedia extractor uses NO caching to handle 100GB+ files
3. **Regex over HTML Parsing:** 60+ patterns provide faster extraction than DOM parsing
4. **Multiple IDF Methods:** Pluggable scoring strategies for relevance tuning
5. **File-backed State:** Frontier and dedup ledger enable resumable crawls
6. **Loose Coupling:** Stages communicate through filesystem, enabling independent reruns
7. **Docker Containerization:** Reproducible Spark environment (linux/amd64 for ARM64 Macs)

---

## Repository Statistics

- **Python Files:** ~50 source files
- **Core Code:** ~15K lines
- **Test Coverage:** 4 test files with 60+ test cases
- **Documentation:** 17+ markdown files
- **Configuration:** Single YAML with 100+ parameters

---

This codebase represents a production-grade information extraction and retrieval system designed for research on GitHub repository analysis with Wikipedia knowledge enrichment.
