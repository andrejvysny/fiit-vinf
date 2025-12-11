# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-stage data pipeline for analyzing public GitHub content with Wikipedia enrichment:

1. **Crawler** (`crawler/`) – Fetches HTML from GitHub, writes crawl metadata
2. **HTML Extractor** (`extractor/` + `spark/jobs/html_extractor.py`) – Extracts entities and text from HTML
3. **Indexer** (`indexer/`) – Builds inverted indexes and enables TF-IDF search
4. **Wikipedia Extractor** (`spark/jobs/wiki_extractor.py`) – Extracts structured data from 100GB+ Wikipedia XML dumps
5. **Entity-Wiki Join** (`spark/jobs/join_html_wiki.py`) – Batch join of HTML entities with Wikipedia
6. **PyLucene Indexer** (`lucene_indexer/`) – Full-text search with PyLucene

Stages are loosely coupled through `workspace/` directory.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Core Commands

### Unified CLI (Recommended)

```bash
bin/cli --help                              # Show available commands
bin/cli pipeline                            # Run full pipeline
bin/cli pipeline --sample 100 --wiki-max-pages 1000  # Test mode
```

### Individual Pipeline Stages

```bash
# 1. Crawl GitHub
python -m crawler --config config.yml

# 2. Extract entities from HTML (Spark via Docker)
bin/cli extract                             # Full extraction
bin/cli extract --sample 100                # Test with 100 files
bin/cli extract --local                     # Fallback to Python (no Docker)

# 3. Extract Wikipedia data
bin/cli wiki --wiki-max-pages 100           # Test with 100 pages
bin/cli wiki                                # Full extraction

# 4. Join entities with Wikipedia
bin/cli join                                # Full join
bin/cli join --entities-max-rows 1000       # Test with 1000 entities

# 5. Build PyLucene index
bin/cli lucene-build                        # Build full index
bin/cli lucene-build --limit 1000           # Test with 1000 docs

# 6. Search the index
bin/cli lucene-search "python web"
bin/cli lucene-search --query "python AND docker" --type boolean
```

### Direct Docker Compose

```bash
docker compose run --rm spark-extract
docker compose run --rm spark-wiki
docker compose run --rm spark-join
docker compose run --rm lucene-build
docker compose run --rm lucene-search
```

## Testing

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

## Architecture

### Data Flow

```
config.yml → crawler → workspace/store/html/
workspace/store/html/ → extractor → workspace/store/text/ + entities.tsv
wiki_dump/*.xml → wiki_extractor → workspace/store/wiki/*.tsv
entities.tsv + wiki/*.tsv → join_html_wiki → workspace/store/join/
text + entities + join → lucene_indexer → workspace/store/lucene_index/
```

### Key Components

| Module | Entry Point | Purpose |
|--------|-------------|---------|
| `crawler/` | `python -m crawler` | Policy-aware frontier crawler |
| `extractor/` | `python -m extractor` | HTML to text + entity extraction |
| `indexer/` | `python -m indexer.build` | TF-IDF inverted index |
| `spark/jobs/` | `bin/cli extract/wiki/join` | Distributed Spark processing |
| `lucene_indexer/` | `bin/cli lucene-*` | PyLucene full-text search |

### Workspace Layout

| Path | Purpose |
|------|---------|
| `workspace/store/html/` | Crawled HTML (SHA256 names) |
| `workspace/store/text/` | Extracted text files |
| `workspace/store/spark/entities/entities.tsv` | Entity annotations |
| `workspace/store/wiki/*.tsv` | Wikipedia structured data (6 files) |
| `workspace/store/wiki/text/` | Wikipedia full article text |
| `workspace/store/join/html_wiki.tsv` | Entity-Wiki join results |
| `workspace/store/lucene_index/` | PyLucene index |
| `workspace/store/index/` | TF-IDF index |

### Entity Types

Entities extracted from GitHub HTML (`entities.tsv` columns: `entity_type`, `value`, `entity`, `file_id`):

| Type | Description | Example |
|------|-------------|---------|
| `stars` | Star count | `1234` |
| `forks` | Fork count | `567` |
| `language` | Programming language | `Python` |
| `license` | SPDX license ID | `MIT`, `Apache-2.0` |
| `readme` | README presence | `true` |
| `topics` | GitHub topic tags | `machine-learning` |
| `url` | Hyperlinks in content | `https://...` |
| `email` | Contact emails | `user@example.com` |

### Wikipedia TSV Schemas

| File | Columns |
|------|---------|
| `pages.tsv` | `page_id`, `title`, `redirect_target` |
| `categories.tsv` | `page_id`, `category` |
| `links.tsv` | `source_id`, `target_title` |
| `infobox.tsv` | `page_id`, `field`, `value` |
| `abstract.tsv` | `page_id`, `abstract` |
| `aliases.tsv` | `alias`, `target_title` |

## Critical Constraints

- **Wikipedia extractor**: Uses streaming with NO caching. Never add `.cache()` calls.
- **Topics streaming join**: Requires Java 11 or 17 (not 24). Check with `java -version`.
- **Regex changes**: Must have test coverage in `tests/test_regexes.py`.
- **Entity TSV**: Streaming-safe, headers emitted once per invocation.
- **Document IDs**: Derived from HTML filename stems for cross-stage consistency.

## Spark Memory Configuration

```bash
# HTML extraction (8-16GB system)
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=4g bin/cli extract

# Wikipedia extraction (16-32GB system)
SPARK_DRIVER_MEMORY=8g SPARK_EXECUTOR_MEMORY=4g bin/cli wiki

# For OOM errors: increase driver memory and partitions
SPARK_DRIVER_MEMORY=12g PARTITIONS=512 bin/cli wiki
```

## Configuration

`config.yml` controls all stages:
- `workspace` – Root path for artifacts
- `crawler` – Seeds, user agents, scope rules, rate limits
- `extractor` – Input/output paths, feature toggles
- `indexer` – IDF methods, token settings

## CLI Commands Reference

All commands via `bin/cli`:

| Command | Purpose |
|---------|---------|
| `extract` | HTML → text + entities (Spark) |
| `wiki` | Wikipedia dump → TSV |
| `join` | Entity ↔ Wikipedia join |
| `join-topics` | Streaming join with topic filtering |
| `lucene-build` | Build PyLucene index |
| `lucene-search` | Query Lucene index |
| `lucene-compare` | Compare TF-IDF vs Lucene |
| `pipeline` | Run full pipeline |
| `stats` | Show pipeline statistics |

## Debugging

```bash
cat workspace/logs/crawler.log              # Crawler logs
cat logs/wiki_extract.jsonl | jq .          # Wiki extraction logs
docker logs vinf-spark-extract              # Docker container logs
open http://localhost:4040                  # Spark UI (while job running)
```

## Key Source Files

| Purpose | Files |
|---------|-------|
| Entity regex patterns | `extractor/regexes.py`, `spark/lib/regexes.py` |
| Wikipedia parsing | `spark/lib/wiki_regexes.py` |
| Spark jobs | `spark/jobs/html_extractor.py`, `wiki_extractor.py`, `join_html_wiki.py` |
| Lucene schema | `lucene_indexer/schema.py` |
| Config loading | `config_loader.py` |
| CLI entry point | `bin/cli` |

## Performance Expectations

| Stage | 500 pages | 50K pages | Full (6M) |
|-------|-----------|-----------|-----------|
| Wiki extraction | ~2s | ~5min | 2-4 hours |
| HTML extraction | ~1s | ~1s | ~1s |
| Join | <1s | ~1min | 10-30min |
| Lucene build | ~20s | ~20s | ~20s |
