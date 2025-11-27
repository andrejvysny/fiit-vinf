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

### Unified CLI (Recommended)

All pipeline operations are available through the unified CLI:

```bash
# Show available commands
bin/cli --help

# Run full pipeline
bin/cli pipeline

# Run full pipeline with test data
bin/cli pipeline --sample 100 --wiki-max-pages 1000
```

### Pipeline Stages (via CLI)

```bash
# 1. Crawl GitHub
python -m crawler --config config.yml

# 2. Extract entities from HTML (Spark via Docker)
bin/cli extract                           # Full extraction
bin/cli extract --sample 100              # Test with 100 files
bin/cli extract --sample 100 --force      # Force re-extraction
bin/cli extract --local                   # Fallback to Python (no Docker)

# 3. Extract Wikipedia data
bin/cli wiki --wiki-max-pages 100         # Test with 100 pages
bin/cli wiki                              # Full extraction

# 4. Join entities with Wikipedia
bin/cli join                              # Full join
bin/cli join --entities-max-rows 1000     # Test with 1000 entities

# 5. Build PyLucene index
bin/cli lucene-build                      # Build full index
bin/cli lucene-build --limit 1000         # Test with 1000 docs

# 6. Search the index
bin/cli lucene-search "python web"
bin/cli lucene-search --query "python AND docker" --type boolean
bin/cli lucene-search --type range --field star_count --min-value 1000

# 7. Compare TF-IDF vs Lucene
bin/cli lucene-compare
```

### Direct Docker Compose (Alternative)

```bash
# Run individual services directly
docker compose run --rm spark-extract
docker compose run --rm spark-wiki
docker compose run --rm spark-join
docker compose run --rm lucene-build
docker compose run --rm lucene-search
docker compose run --rm lucene-compare
```

### Streaming Topics Join (requires local Spark)

```bash
# Streaming join for TOPICS with relevance filtering
bin/cli join-topics
bin/cli join-topics --maxFilesPerTrigger 8    # Lower memory usage
bin/cli join-topics --clean                    # Reset checkpoints
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

| Command | Flag | Purpose |
|---------|------|---------|
| `bin/cli extract` | `--sample N` | Process first N files |
| `bin/cli extract` | `--force` | Overwrite outputs |
| `bin/cli extract` | `--dry-run` | List files without processing |
| `bin/cli extract` | `--local` | Fallback to Python (no Docker) |
| `bin/cli wiki` | `--wiki-max-pages N` | Limit pages for testing |
| `bin/cli wiki` | `--no-text` | Skip full text extraction |
| `bin/cli join` | `--entities-max-rows N` | Limit entities for testing |
| `bin/cli join-topics` | `--maxFilesPerTrigger N` | Bounded memory (default: 16) |
| `bin/cli join-topics` | `--clean` | Reset checkpoints |
| `bin/cli lucene-build` | `--limit N` | Limit docs for PyLucene index |
| `bin/cli lucene-search` | `--type TYPE` | Query type (simple/boolean/range/phrase/fuzzy) |
| `bin/cli lucene-search` | `--top N` | Number of results |
| `bin/cli pipeline` | `--skip-extract` | Skip HTML extraction step |
| `bin/cli pipeline` | `--skip-wiki` | Skip Wikipedia extraction step |
| `bin/cli pipeline` | `--skip-join` | Skip entity-wiki join step |
| `bin/cli pipeline` | `--skip-lucene` | Skip Lucene index build step |

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
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=4g bin/cli extract

# Wikipedia extraction (16-32GB system)
SPARK_DRIVER_MEMORY=8g SPARK_EXECUTOR_MEMORY=4g bin/cli wiki

# For OOM errors: increase driver memory and partitions
SPARK_DRIVER_MEMORY=12g PARTITIONS=512 bin/cli wiki

# Entity-Wiki join
SPARK_DRIVER_MEMORY=6g SPARK_EXECUTOR_MEMORY=3g bin/cli join
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
