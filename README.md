# VINF Pipeline

Data processing pipeline for analyzing GitHub repositories with Wikipedia enrichment.

## Quick Start

```bash
# Run full pipeline (test mode)
bin/cli pipeline --sample 100 --wiki-max-pages 1000

# Run full pipeline (production)
bin/cli pipeline
```

## Requirements

- Docker & Docker Compose
- Python 3.9+ (for local development)
- Wikipedia XML dump in `wiki_dump/` (for wiki extraction)
- Crawled HTML files in `workspace/store/html/` (for extraction)

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Pipeline Overview

```
HTML files → Extract → Entities + Text
Wiki dump  → Extract → Wiki TSV files
                ↓
         Entity-Wiki Join
                ↓
         Lucene Index → Search
```

---

## Commands Reference

### 1. HTML Extraction (Spark)

Extract text and entities from crawled GitHub HTML pages.

```bash
# Full extraction
bin/cli extract

# Test with 100 files
bin/cli extract --sample 100

# Force re-extraction
bin/cli extract --force

# Custom partitions (for performance tuning)
bin/cli extract --partitions 256

# Dry run (list files only)
bin/cli extract --dry-run
```

**Options:**
| Flag | Description |
|------|-------------|
| `--sample N` | Process only first N files |
| `--partitions N` | Spark partitions (default: 128) |
| `--force` | Overwrite existing outputs |
| `--dry-run` | List files without processing |
| `--local` | Use Python extractor (no Docker) |

**Input:** `workspace/store/html/`

**Output:**
- `workspace/store/text/` - Plain text files
- `workspace/store/spark/entities/entities.tsv` - Entity annotations

---

### 2. Wikipedia Extraction (Spark)

Extract structured data from Wikipedia XML dumps.

```bash
# Test with 100 pages
bin/cli wiki --wiki-max-pages 100

# Full extraction (requires 8GB+ RAM)
bin/cli wiki

# More partitions for large dumps
bin/cli wiki --partitions 512

# Skip full text extraction (faster)
bin/cli wiki --no-text
```

**Options:**
| Flag | Description |
|------|-------------|
| `--wiki-max-pages N` | Limit pages to process |
| `--partitions N` | Spark partitions (default: 256) |
| `--no-text` | Skip full text extraction |
| `--dry-run` | List files without processing |

**Input:** `wiki_dump/*.xml` or `wiki_dump/*.xml.bz2`

**Output:** `workspace/store/wiki/`
- `pages.tsv` - Page metadata
- `categories.tsv` - Category assignments
- `links.tsv` - Internal wiki links
- `infobox.tsv` - Infobox data
- `abstract.tsv` - Article abstracts
- `aliases.tsv` - Redirect mappings
- `text/` - Full article text files

---

### 3. Entity-Wiki Join (Spark)

Join extracted entities with Wikipedia data.

```bash
# Full join
bin/cli join

# Test with 1000 entities
bin/cli join --entities-max-rows 1000

# Custom partitions
bin/cli join --partitions 128
```

**Options:**
| Flag | Description |
|------|-------------|
| `--entities-max-rows N` | Limit entity rows |
| `--partitions N` | Spark partitions (default: 64) |
| `--dry-run` | Preview without writing |

**Input:**
- `workspace/store/spark/entities/entities.tsv`
- `workspace/store/wiki/*.tsv`

**Output:** `workspace/store/join/html_wiki.tsv`

---

### 4. Build Lucene Index

Build PyLucene full-text search index.

```bash
# Build full index
bin/cli lucene-build

# Test with 1000 documents
bin/cli lucene-build --limit 1000
```

**Options:**
| Flag | Description |
|------|-------------|
| `--limit N` | Limit documents to index |
| `--dry-run` | Preview without indexing |

**Input:**
- `workspace/store/text/`
- `workspace/store/spark/entities/entities.tsv`
- `workspace/store/join/html_wiki.tsv`

**Output:** `workspace/store/lucene_index/`

---

### 5. Search Lucene Index

Search the index with various query types.

#### Simple Search (default)
```bash
bin/cli lucene-search "python web"
bin/cli lucene-search --query "docker container" --top 20
```

#### Boolean Search
```bash
bin/cli lucene-search --query "python AND docker" --type boolean
bin/cli lucene-search --query "web OR api" --type boolean
bin/cli lucene-search --query "python NOT java" --type boolean
```

#### Phrase Search
```bash
bin/cli lucene-search --query "machine learning" --type phrase
```

#### Fuzzy Search (typo-tolerant)
```bash
bin/cli lucene-search --query "pyhton" --type fuzzy
bin/cli lucene-search --query "javscript" --type fuzzy --max-edits 2
```

#### Range Search (numeric fields)
```bash
# Repos with 1000+ stars
bin/cli lucene-search --type range --field star_count --min-value 1000

# Repos with 100-500 forks
bin/cli lucene-search --type range --field fork_count --min-value 100 --max-value 500
```

**Options:**
| Flag | Description |
|------|-------------|
| `--query TEXT` | Search query |
| `--type TYPE` | Query type: simple, boolean, phrase, fuzzy, range |
| `--top N` | Number of results (default: 10) |
| `--field FIELD` | Field for range queries |
| `--min-value N` | Min value for range |
| `--max-value N` | Max value for range |
| `--max-edits N` | Max edits for fuzzy |
| `--json` | Output as JSON |

---

### 6. Compare TF-IDF vs Lucene

Compare search results between TF-IDF and Lucene indexes.

```bash
bin/cli lucene-compare
```

**Input:**
- `queries.txt` - Test queries (one per line)
- `workspace/store/index/` - TF-IDF index
- `workspace/store/lucene_index/` - Lucene index

**Output:** `reports/index_comparison.md`

---

### 7. Pipeline Statistics

Show statistics about pipeline artifacts.

```bash
bin/cli stats
```

---

### 8. Full Pipeline

Run all stages automatically.

```bash
# Full pipeline
bin/cli pipeline

# Test mode
bin/cli pipeline --sample 100 --wiki-max-pages 1000

# Skip specific stages
bin/cli pipeline --skip-extract
bin/cli pipeline --skip-wiki
bin/cli pipeline --skip-join
bin/cli pipeline --skip-lucene

# Only join and index (skip extraction)
bin/cli pipeline --skip-extract --skip-wiki
```

**Options:**
| Flag | Description |
|------|-------------|
| `--sample N` | Sample size for HTML extraction |
| `--wiki-max-pages N` | Limit wiki pages |
| `--skip-extract` | Skip HTML extraction |
| `--skip-wiki` | Skip Wikipedia extraction |
| `--skip-join` | Skip entity-wiki join |
| `--skip-lucene` | Skip Lucene index build |

---

## Direct Docker Commands

You can also run services directly via Docker Compose:

```bash
# Spark services
docker compose run --rm spark-extract
docker compose run --rm spark-wiki
docker compose run --rm spark-join

# Lucene services
docker compose run --rm lucene-build
docker compose run --rm lucene-search
docker compose run --rm lucene-compare
```

---

## Memory Configuration

For large datasets, configure Spark memory:

```bash
# HTML extraction (16GB system)
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=4g bin/cli extract

# Wikipedia extraction (32GB system)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g bin/cli wiki

# Entity-Wiki join
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=3g bin/cli join
```

| Variable | Description | Default |
|----------|-------------|---------|
| `SPARK_DRIVER_MEMORY` | Driver JVM memory | 6g |
| `SPARK_EXECUTOR_MEMORY` | Executor JVM memory | 4g |
| `PARTITIONS` | Number of partitions | varies |

---

## Workspace Structure

```
workspace/
├── store/
│   ├── html/                    # Crawled HTML files (input)
│   ├── text/                    # Extracted text files
│   ├── spark/entities/          # Entity annotations
│   ├── wiki/                    # Wikipedia structured data
│   │   ├── pages.tsv
│   │   ├── categories.tsv
│   │   ├── links.tsv
│   │   ├── infobox.tsv
│   │   ├── abstract.tsv
│   │   ├── aliases.tsv
│   │   └── text/
│   ├── join/                    # Entity-Wiki join results
│   │   └── html_wiki.tsv
│   ├── index/                   # TF-IDF index
│   └── lucene_index/            # PyLucene index
└── logs/
```

---

## Testing

```bash
# Run all tests
python -m unittest discover tests

# Run specific tests
python -m unittest tests.test_regexes
python -m unittest tests.test_wiki_regexes
python -m unittest tests.test_spark_extractor
```

---

## Debugging

```bash
# Spark UI (while job running)
open http://localhost:4040

# Container logs
docker logs vinf-spark-extract
docker logs vinf-lucene-build

# Job logs
cat logs/wiki_extract.jsonl | jq .
cat logs/wiki_join.jsonl | jq .
```

---

## Examples

### Complete Test Run

```bash
# 1. Extract from 100 HTML files
bin/cli extract --sample 100

# 2. Extract from 1000 wiki pages
bin/cli wiki --wiki-max-pages 1000

# 3. Join entities with wiki
bin/cli join

# 4. Build search index
bin/cli lucene-build

# 5. Search
bin/cli lucene-search "python machine learning"
```

### Production Run

```bash
# Full pipeline with monitoring
bin/cli pipeline

# Or step by step with custom memory
SPARK_DRIVER_MEMORY=8g bin/cli extract
SPARK_DRIVER_MEMORY=12g bin/cli wiki
bin/cli join
bin/cli lucene-build
```

---

## Crawling

Run the GitHub crawler (requires config.yml):

```bash
python -m crawler --config config.yml
```

---

## Project Structure

```
bin/cli                 # Unified CLI (main entry point)
crawler/                # GitHub HTML crawler
extractor/              # HTML to text + entity extraction
indexer/                # TF-IDF indexing
lucene_indexer/         # PyLucene full-text search
spark/jobs/             # Spark processing jobs
  ├── html_extractor.py
  ├── wiki_extractor.py
  └── join_html_wiki.py
spark/lib/              # Shared utilities
tests/                  # Unit tests
config.yml              # Configuration
docker-compose.yml      # Docker orchestration
```

---

## Documentation

- `CLAUDE.md` - Developer reference for Claude Code
- `FULL_RUN_GUIDE.md` - Complete pipeline tutorial
- `docs/` - Additional documentation
