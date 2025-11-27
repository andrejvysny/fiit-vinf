# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-stage data pipeline for analyzing public GitHub content with Wikipedia enrichment:

1. **Crawler** (`crawler/`) – Fetches HTML from GitHub, writes crawl metadata
2. **HTML Extractor** (`extractor/` + `spark/jobs/html_extractor.py`) – Extracts entities and text from HTML
3. **Indexer** (`indexer/`) – Builds inverted indexes and enables TF-IDF search
4. **Wikipedia Extractor** (`spark/jobs/wiki_extractor.py`) – Extracts structured data from 100GB+ Wikipedia XML dumps
5. **Entity-Wiki Join** (`spark/jobs/join_html_wiki.py`) – Batch join of HTML entities with Wikipedia
6. **Topics-Wiki Join** (`spark/jobs/join_html_wiki_topics.py`) – Streaming join for TOPICS with relevance filtering

Stages are loosely coupled through `workspace/` directory.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Core Commands

### Pipeline Stages

```bash
# 1. Crawl GitHub
python -m crawler --config config.yml

# 2. Extract entities (Spark preferred, --local for fallback)
bin/spark_extract --config config.yml
bin/spark_extract --local --config config.yml  # Without Docker

# 3. Build search index
python3 -m indexer.build --config config.yml

# 4. Query index
python3 -m indexer.query --config config.yml --query "your terms"
```

### Wikipedia Pipeline

```bash
# Test first (100 pages)
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# Full extraction (requires 16-32GB RAM, 2-3 hours)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g bin/spark_wiki_extract --partitions 512

# Batch join (entities → Wikipedia)
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# Streaming TOPICS join (requires Java 11 or 17, not 24)
bin/spark_join_wiki_topics \
  --entities workspace/store/spark/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/wiki/join
```

### PyLucene Index (Advanced Queries)

```bash
# Build PyLucene index
python -m lucene_indexer.build \
  --text-dir workspace/store/text \
  --entities workspace/store/entities/entities.tsv \
  --wiki-join workspace/store/join/html_wiki.tsv \
  --output workspace/store/lucene_index

# Search with different query types
python -m lucene_indexer.search --query "python web" --type simple
python -m lucene_indexer.search --query "python AND docker" --type boolean
python -m lucene_indexer.search --field star_count --min-value 1000 --type range
python -m lucene_indexer.search --query "machine learning" --type phrase
python -m lucene_indexer.search --query "pyhton" --type fuzzy

# Compare TF-IDF vs Lucene
python -m lucene_indexer.compare --queries "python,docker,web" --output reports/comparison.md
```

### Testing

```bash
# Run all tests
python -m unittest discover tests

# Run specific test files
python -m unittest tests.test_regexes
python -m unittest tests.test_link_extractor
python -m unittest tests.test_wiki_regexes
python -m unittest tests.test_spark_extractor

# Run single test method
python -m unittest tests.test_regexes.TestRegexes.test_stars_pattern
```

## Key CLI Flags

| Tool | Flag | Purpose |
|------|------|---------|
| `bin/spark_extract` | `--sample N` | Process first N files |
| `bin/spark_extract` | `--force` | Overwrite outputs |
| `bin/spark_extract` | `--dry-run` | List files without processing |
| `bin/spark_extract` | `--local` | Fallback to Python (no Docker) |
| `bin/spark_wiki_extract` | `--wiki-max-pages N` | Limit pages for testing |
| `bin/spark_wiki_extract` | `--no-text` | Skip full text extraction |
| `bin/spark_join_wiki` | `--entities-max-rows N` | Limit entities for testing |
| `bin/spark_join_wiki_topics` | `--maxFilesPerTrigger N` | Bounded memory (default: 16) |
| `bin/spark_join_wiki_topics` | `--clean` | Reset checkpoints |
| `indexer.build` | `--limit N` | Process N documents |
| `indexer.query` | `--top N` | Return top N results |
| `lucene_indexer.build` | `--limit N` | Limit docs for PyLucene index |
| `lucene_indexer.search` | `--type TYPE` | Query type (simple/boolean/range/phrase/fuzzy) |
| `lucene_indexer.compare` | `--queries Q` | Compare TF-IDF vs Lucene results |

## Architecture

### Data Flow

```
config.yml → crawler → workspace/store/html/
                     → workspace/metadata/crawl_metadata.jsonl

workspace/store/html/ → extractor → workspace/store/text/
                                  → workspace/store/entities/entities.tsv

workspace/store/text/ → indexer.build → workspace/store/index/

wiki_dump/*.xml → wiki_extractor → workspace/store/wiki/*.tsv
                                 → workspace/store/wiki/text/*.txt

entities.tsv + wiki/*.tsv → join_html_wiki → workspace/store/join/
                          → join_html_wiki_topics → workspace/store/wiki/join/
```

### Key Components

**Crawler** (`crawler/`):
- `service.py` – Main `CrawlerScraperService` orchestrator
- `crawl_frontier.py` – File-backed URL queue with dedup
- `crawl_policy.py` – Robots.txt-aware policy
- `unified_fetcher.py` – HTTP retrieval with retries

**HTML Extractor** (`extractor/`):
- `pipeline.py` – Main `ExtractorPipeline`
- `entity_extractors.py` – GitHub metadata extraction (stars, forks, languages)
- `regexes.py` – Entity detection patterns (test coverage required)
- `html_clean.py` – HTML cleaning

**Wikipedia Extractor** (`spark/jobs/`):
- `wiki_extractor.py` – Streaming XML extraction (NO caching)
- `join_html_wiki.py` – Batch entity-Wikipedia join
- `join_html_wiki_topics.py` – Structured Streaming TOPICS join
- `spark/lib/wiki_regexes.py` – MediaWiki parsing patterns

**Indexer** (`indexer/`):
- `build.py` – Index builder CLI
- `query.py` – Search CLI
- `search.py` – TF-IDF scoring engine
- `idf.py` – IDF strategies (classic, smoothed, probabilistic, max)

**PyLucene Indexer** (`lucene_indexer/`):
- `schema.py` – Index schema with field definitions and justifications
- `build.py` – Index builder with entity and wiki enrichment
- `search.py` – Search engine with Boolean, Range, Phrase, Fuzzy queries
- `compare.py` – Query comparison between TF-IDF and Lucene indexes

### Workspace Layout

| Path | Producer | Purpose |
|------|----------|---------|
| `workspace/state/frontier.jsonl` | crawler | Pending URLs queue |
| `workspace/state/fetched_urls.txt` | crawler | Dedup ledger |
| `workspace/store/html/` | crawler | HTML snapshots (SHA256 names) |
| `workspace/store/text/` | extractor | Raw text files |
| `workspace/store/entities/entities.tsv` | extractor | Entity annotations |
| `workspace/store/index/` | indexer | TF-IDF index artifacts |
| `workspace/store/lucene_index/` | lucene_indexer | PyLucene index |
| `workspace/store/wiki/*.tsv` | wiki_extractor | Wikipedia structured data (7 files) |
| `workspace/store/wiki/text/*.txt` | wiki_extractor | Full article text (SHA256 names) |
| `workspace/store/join/*.tsv` | join_html_wiki | Batch join results |
| `workspace/store/wiki/join/` | join_html_wiki_topics | Streaming join results |

## Implementation Notes

### Spark Memory Configuration

```bash
# HTML extraction (8-16GB system)
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=4g bin/spark_extract

# Wikipedia extraction (16-32GB system)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g bin/spark_wiki_extract

# For OOM errors: increase driver memory and partitions
SPARK_DRIVER_MEMORY=16g bin/spark_wiki_extract --partitions 1024
```

### Critical Constraints

- **Wikipedia extractor**: Uses streaming with NO caching. Never add `.cache()` calls.
- **Topics streaming join**: Requires Java 11 or 17 (not 24). Check with `java -version`.
- **Regex changes**: Must have test coverage in `tests/test_regexes.py`.
- **Entity TSV**: Streaming-safe, headers emitted once per invocation.
- **Document IDs**: Derived from HTML filename stems for cross-stage consistency.

### Debugging

```bash
# Check logs
cat workspace/logs/crawler.log
cat logs/wiki_extract.jsonl | jq .
cat logs/wiki_join.jsonl | jq .

# Spark UI (while job running)
open http://localhost:4040

# Docker container logs
docker logs vinf-spark-extractor

# Check join statistics
cat workspace/store/join/join_stats.json | jq .
```

## Configuration (`config.yml`)

Single YAML controls all stages:
- `workspace` – Root path for artifacts
- `crawler` – Seeds, user agents, scope rules, rate limits
- `extractor` – Input/output paths, feature toggles
- `indexer` – IDF methods, token settings

Override with `--config /path/to/config.yml` or individual CLI flags.
