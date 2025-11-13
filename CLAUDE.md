# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-stage data pipeline for analyzing public GitHub content with Wikipedia enrichment:

### Core Pipeline
1. **Crawler** – Fetches HTML from GitHub and writes crawl metadata
2. **HTML Extractor** – Extracts entities and text from stored HTML
3. **Indexer** – Builds inverted indexes and enables search

### Wikipedia Extension Pipeline
4. **Wikipedia Extractor** – Extracts structured data from Wikipedia XML dumps (100GB+) with optional full text
5. **Entity-Wiki Join (Batch)** – Joins HTML entities with Wikipedia canonical data for entity resolution
6. **Topics-Wiki JOIN (Streaming)** – **NEW**: Streams GitHub TOPICS entities and joins with relevant Wikipedia articles using category/abstract filtering

The pipeline is loosely coupled through the `workspace/` directory, allowing independent execution or resumption of each stage.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

All commands assume the virtual environment is active.

## Core Commands

### Running the Core Pipeline

```bash
# 1. Crawl GitHub (writes to workspace/store/html/)
python -m crawler --config config.yml

# 2. Extract entities and text (two options):

# Option A: PySpark extractor (preferred for large datasets)
bin/spark_extract --config config.yml

# Option B: Legacy single-process extractor
python -m extractor --config config.yml

# Fall back to legacy extractor if Docker unavailable:
bin/spark_extract --local --config config.yml

# 3. Build search index
python3 -m indexer.build --config config.yml

# 4. Query the index
python3 -m indexer.query --config config.yml --query "your search terms"
```

### Running the Wikipedia Extension Pipeline

```bash
# 1. Extract Wikipedia data from XML dump (100GB+, requires 16-32GB RAM)
# Test first:
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# Full extraction (~2-3 hours):
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512

# 2. Join HTML entities with Wikipedia (batch)
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# 3. Join TOPICS with Wikipedia (streaming, requires Java 11 or 17)
bin/spark_join_wiki_topics \
  --entities workspace/store/spark/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/wiki/join
```

**Key Differences**:
- Wikipedia extraction runs standalone (no config.yml dependency)
- Uses streaming architecture to handle 100GB+ files without OOM errors
- Topics JOIN uses PySpark Structured Streaming for incremental processing
- All commands are wrapper scripts around Spark
- Full Wikipedia extraction requires significantly more RAM than HTML extraction
- **Java Compatibility**: Streaming jobs require Java 11 or 17 (not Java 24 due to Spark 4.0.1 limitations)

### Testing

```bash
# Run all tests
python -m unittest discover tests

# Run specific test files
python -m unittest tests.test_regexes
python -m unittest tests.test_link_extractor
```

### Useful Flags

**HTML Extractor (`bin/spark_extract`):**
- `--sample N` – Process only first N files (for smoke tests)
- `--force` – Overwrite existing outputs
- `--dry-run` – List files without processing
- `--partitions N` – Number of Spark partitions (default: 64)
- `--local` – Fallback to Python extractor (no Docker)
- `--config PATH` – Custom config file

**Wikipedia Extractor (`bin/spark_wiki_extract`):**
- `--wiki-max-pages N` – Process only first N pages (for testing, default: all)
- `--wiki-in DIR` – Input directory with Wikipedia dumps (default: wiki_dump)
- `--out DIR` – Output directory (default: workspace/store/wiki)
- `--partitions N` – Number of Spark partitions (default: 256, auto-scales for large files)
- `--log FILE` – Log file path (default: logs/wiki_extract.jsonl)
- `--dry-run` – List files without processing
- `--extract-text` / `--no-text` – Enable/disable full text extraction (default: enabled)

**Entity-Wikipedia Join (Batch) (`bin/spark_join_wiki`):**
- `--entities FILE` – Path to entities.tsv (required)
- `--wiki DIR` – Directory with Wikipedia TSV files (required)
- `--out DIR` – Output directory (default: workspace/store/join)
- `--entities-max-rows N` – Limit entity rows for testing
- `--partitions N` – Number of Spark partitions (default: 64)
- `--dry-run` – Preview without writing outputs

**Topics-Wikipedia JOIN (Streaming) (`bin/spark_join_wiki_topics`):**
- `--entities FILE` – Path to entities TSV (required)
- `--wiki DIR` – Wiki dimensions directory (required)
- `--out DIR` – Output directory (default: workspace/store/wiki/join)
- `--checkpoint DIR` – Checkpoint directory for streaming state (default: workspace/store/wiki/join/_chkpt/topics)
- `--maxFilesPerTrigger N` – Max files per streaming batch (default: 16, lower for less memory)
- `--relevantCategories KW` – Comma-separated relevance keywords (default: programming,software,computer,library,framework,license)
- `--absHit BOOL` – Enable abstract matching (default: true)
- `--clean` – Clean checkpoint and output dirs

**Indexer Build:**
- `--limit N` – Process only N documents
- `--dry-run` – Calculate stats without writing files
- `--idf-method METHOD` – Choose IDF weighting (classic, smoothed, probabilistic, max)

**Indexer Query:**
- `--top N` – Return top N results (default: 10)
- `--show-path` – Display absolute document paths
- `--idf-method METHOD` – Override IDF method for query

## Architecture Details

### Data Flow

#### Core Pipeline
```
config.yml → crawler → workspace/store/html/
                    → workspace/metadata/crawl_metadata.jsonl
                    → workspace/state/frontier.jsonl

workspace/store/html/ → extractor → workspace/store/text/
                                  → workspace/store/entities/entities.tsv

workspace/store/text/ → indexer.build → workspace/store/index/
                                      → docs.jsonl
                                      → postings.jsonl
                                      → manifest.json

workspace/store/index/ → indexer.query → ranked search results
```

#### Wikipedia Extension Pipeline
```
wiki_dump/*.xml → wiki_extractor (Spark) → workspace/store/wiki/
                                          → pages.tsv
                                          → categories.tsv
                                          → links.tsv
                                          → infobox.tsv
                                          → abstract.tsv
                                          → aliases.tsv
                                          → wiki_text_metadata.tsv (NEW)
                                          → text/*.txt (NEW: full article text)

workspace/store/entities/entities.tsv + workspace/store/wiki/ → join_html_wiki (Spark Batch)
                                                               → workspace/store/join/
                                                               → html_wiki.tsv
                                                               → join_stats.json
                                                               → html_wiki_agg.tsv

workspace/store/spark/entities/entities.tsv + workspace/store/wiki/ → join_html_wiki_topics (Spark Streaming)
                                                                     → workspace/store/wiki/join/
                                                                     → html_wiki_topics_output/*.csv
                                                                     → html_wiki_topics_stats.tsv
                                                                     → _chkpt/topics/ (checkpoints)
```

### Key Components

**Crawler (`crawler/`)**:
- `service.py` – Main `CrawlerScraperService` orchestrates crawling
- `crawl_frontier.py` – File-backed URL queue with deduplication
- `crawl_policy.py` – Robots.txt-aware policy enforcement
- `unified_fetcher.py` – HTTP retrieval with retry logic
- `metadata_writer.py` – JSONL metadata logging

**HTML Extractor (`extractor/`)**:
- `pipeline.py` – Main `ExtractorPipeline` orchestrator
- `entity_extractors.py` – GitHub metadata extraction (stars, forks, languages)
- `regexes.py` – Regex patterns for entity detection
- `html_clean.py` – HTML cleaning and text extraction
- Spark version in `spark/jobs/html_extractor.py` for parallel processing

**Wikipedia Extractor (`spark/jobs/`)**:
- `wiki_extractor.py` – Spark job for Wikipedia XML dump extraction (streaming, no caching) with optional full text
- `join_html_wiki.py` – Spark batch job for entity-Wikipedia join
- `join_html_wiki_topics.py` – **NEW**: Spark Structured Streaming job for TOPICS-Wikipedia join with relevance filtering
- `spark/lib/wiki_regexes.py` – Regex patterns for MediaWiki XML parsing, title normalization, and wikitext cleaning
- Wrapper scripts: `bin/spark_wiki_extract`, `bin/spark_join_wiki`, `bin/spark_join_wiki_topics`

**Indexer (`indexer/`)**:
- `build.py` – CLI for building inverted index
- `query.py` – CLI for searching the index
- `search.py` – TF-IDF scoring engine
- `ingest.py` – Document tokenization and vocabulary building
- `idf.py` – Multiple IDF calculation strategies
- `compare.py` – Generate reports comparing IDF methods

### Spark Extractor Architecture

The Spark-based extractor (`spark/jobs/html_extractor.py`) provides parallel processing:
- Uses Docker Compose to spin up Spark master/worker
- Loads text via `wholeTextFiles` for distributed processing
- Tokenization runs as Spark UDF (`spark/lib/tokenize.py`)
- Regex extraction happens per-partition (`spark/lib/regexes.py`)
- Outputs TSV files: lexicon, postings, docstore, entities
- Writes manifest with SHA-1 checksums for reproducibility

Access via `bin/spark_extract` wrapper script. Use `--local` flag to fall back to single-process extractor if Docker is unavailable.

### Workspace Layout

| Path | Producer | Purpose |
|------|----------|---------|
| `workspace/state/frontier.jsonl` | crawler | Pending URLs queue |
| `workspace/state/fetched_urls.txt` | crawler | Deduplication ledger |
| `workspace/state/service_stats.json` | crawler | Runtime metrics |
| `workspace/store/html/` | crawler | HTML snapshots (SHA256 filenames) |
| `workspace/metadata/crawl_metadata.jsonl` | crawler | Fetch metadata |
| `workspace/store/text/` | extractor | Raw text files |
| `workspace/store/entities/entities.tsv` | extractor | Entity annotations |
| `workspace/store/index/` | indexer | Index artifacts |
| `workspace/store/wiki/*.tsv` | wiki_extractor | Wikipedia structured data (7 TSV files) |
| `workspace/store/wiki/text/*.txt` | wiki_extractor | **NEW**: Full article text (SHA256-named) |
| `workspace/store/join/*.tsv` | join_html_wiki | Entity-Wikipedia batch join results |
| `workspace/store/wiki/join/html_wiki_topics_output/` | join_html_wiki_topics | **NEW**: TOPICS streaming join results |
| `workspace/store/wiki/join/_chkpt/` | join_html_wiki_topics | **NEW**: Streaming checkpoints |
| `workspace/logs/` | all | Module logs |
| `logs/wiki_extract.jsonl` | wiki_extractor | Wikipedia extraction structured logs |
| `logs/wiki_join.jsonl` | join_html_wiki | Join pipeline structured logs |

**Important:** `workspace/` is runtime scratch space. Keep it out of version control.

### Configuration (`config.yml`)

Single YAML file controls all three stages:
- `workspace` – Root path for all artifacts
- `crawler` – Seeds, user agents, scope rules, rate limits, storage paths
- `extractor` – Input/output paths, feature toggles
- `indexer` – Build/query defaults, IDF methods, token counting

Override config file: `--config /path/to/config.yml`
Override individual settings with CLI flags (see `--help` for each module)

## Testing Guidelines

- Tests use Python's `unittest` framework
- Add regression tests for any regex pattern changes in `extractor/regexes.py`
- Keep HTML fixtures deterministic (see `tests/test_regexes.py`)
- Test crawl policy and scope rules with fixtures
- Run full test suite before committing changes

## Important Implementation Notes

### Crawler
- Frontier and fetched URL registries are file-backed and survive restarts
- `CrawlerScraperService.start()` resumes from previous state
- Scope enforcement (allowed hosts, depth limits, per-repo quotas) happens before fetching
- Rate limiting combines request spacing with batch pauses
- Robots.txt cached to `workspace/state/robots_cache.jsonl`

### Extractor
- Document IDs derived from HTML filename stems for consistency across stages
- Entity TSV writing is streaming-safe for large datasets
- Regex patterns in `extractor/regexes.py` must have test coverage in `tests/`
- HTML cleaning removes boilerplate but preserves content structure

### Indexer
- Builds deterministic JSONL/JSON artifacts for easy diffing
- Supports multiple IDF strategies: classic, smoothed, probabilistic, max
- Document records include optional tiktoken counts (`--use-tokens`)
- Query layer supports both TF-IDF and BM25-lite scoring
- Postings include positional information for phrase queries

### Spark Considerations

#### HTML Extractor
- Runs in Docker with local volume mounts
- Uses `wholeTextFiles` for distributed text loading
- Tokenization UDF enables DataFrame-based processing
- Repartitions by term before aggregation to avoid skew
- Writes consolidated TSV files (no Spark part files)
- Generates manifests with SHA-1 checksums for reproducibility

#### Wikipedia Extractor (100GB+ File Handling)
- **CRITICAL**: Uses streaming architecture with NO caching to prevent OOM errors
- Processes pages via `mapPartitions` with buffer limits (max 50K lines per page)
- Auto-scales partitions based on file size (256+ for files > 50GB)
- Uses `rdd.take(N)` for limits instead of processing all data
- Writes each output separately to avoid memory buildup
- **Full text extraction** (optional, enabled by default):
  - Cleans wikitext markup (templates, links, refs, HTML tags) via `clean_wikitext_to_plaintext()`
  - Uses SHA256 content addressing for deduplication (identical articles stored once)
  - Outputs `wiki_text_metadata.tsv` (page_id → SHA256 mapping) and `text/*.txt` files
  - Template removal limited to max 10 iterations to prevent infinite loops
  - Use `--no-text` to disable if only structured data needed
- Memory configuration:
  - Test mode (≤1000 pages): 4g driver, 2g executor
  - Full mode: 12g driver, 6g executor (minimum for 100GB+ dumps)
  - Off-heap memory: 2g
  - Memory fraction: 0.8 for large files
- **Do NOT** add `.cache()` calls to Wikipedia extraction code - this will cause OOM

#### Entity-Wikipedia Join (Batch)
- Caches canonical mapping (small, < 500MB) for reuse
- Uses Spark AQE for adaptive query optimization
- TOPICS entities are exploded (comma-separated → multiple rows)
- Confidence scoring via UDF
- Typical memory: 6g driver, 3g executor
- Much faster than extraction (< 10 minutes)

#### Topics-Wikipedia JOIN (Structured Streaming)
- **NEW**: Spark Structured Streaming architecture for incremental processing
- Bounded memory via `maxFilesPerTrigger` parameter (default: 16 files per batch)
- Streams TOPICS entities and joins with static Wikipedia dimensions (pages, aliases, categories, abstracts)
- Multi-hop join logic:
  1. Normalize topic text (lowercase, ASCII-fold, punct collapse)
  2. Resolve aliases (redirects) to canonical titles
  3. Join to pages by normalized title
  4. Aggregate categories by page_id
  5. Join abstracts by page_id
  6. Filter for relevance (category keywords OR abstract contains entity)
- Relevance filtering:
  - Category substring matching (default keywords: programming, software, computer, library, framework, license)
  - Abstract text contains normalized entity value
- Confidence scoring: exact+cat, alias+cat, exact+abs, alias+abs
- No broadcasts or large in-memory collections
- Checkpoint-based state management for resumability
- Per-batch statistics via `foreachBatch`
- Typical memory: 4g driver, 2g executor
- Duration: 2-5 minutes for full dataset
- **Java requirement**: Java 11 or 17 (Java 24 incompatible with Spark 4.0.1)

## Common Operational Patterns

**Starting a new crawl:**
1. Update `config.yml` seeds and scope rules
2. Clear or backup `workspace/state/` if starting fresh
3. Run crawler and monitor `workspace/logs/crawler.log`
4. Check `workspace/state/service_stats.json` for metrics

**Iterating on HTML extraction rules:**
1. Modify patterns in `extractor/regexes.py`
2. Add test cases in `tests/test_regexes.py`
3. Run `python -m unittest tests.test_regexes`
4. Test on small sample: `bin/spark_extract --sample 100`
5. Use `--force` to overwrite existing outputs

**Running Wikipedia extraction:**
1. Download Wikipedia dump to `wiki_dump/` directory (104GB uncompressed XML)
2. Test with small sample: `bin/spark_wiki_extract --wiki-max-pages 100`
3. Verify outputs created: `ls -lh workspace/store/wiki/`
4. Run full extraction (requires 16-32GB RAM): `SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g bin/spark_wiki_extract --partitions 512`
5. Expected duration: 2-3 hours for ~7M pages
6. Monitor progress via Spark UI: http://localhost:4040

**Running entity-Wikipedia join (batch):**
1. Ensure both inputs exist: `workspace/store/entities/entities.tsv` and `workspace/store/wiki/*.tsv`
2. Test with sample: `bin/spark_join_wiki --entities workspace/store/entities/entities.tsv --wiki workspace/store/wiki --out workspace/store/join --entities-max-rows 10000`
3. Check match statistics: `cat workspace/store/join/join_stats.json | jq .`
4. Run full join: `bin/spark_join_wiki --entities workspace/store/entities/entities.tsv --wiki workspace/store/wiki --out workspace/store/join`

**Running Topics-Wikipedia streaming join:**
1. **Check Java version**: `java -version` (must be Java 11 or 17, not 24)
2. If Java 24, install Java 17: `brew install openjdk@17 && export JAVA_HOME=$(/usr/libexec/java_home -v 17)`
3. Ensure inputs exist: `workspace/store/spark/entities/entities.tsv` and `workspace/store/wiki/*.tsv`
4. Test with one file per batch: `bin/spark_join_wiki_topics --maxFilesPerTrigger 1`
5. Check output: `ls -lh workspace/store/wiki/join/` (should have `html_wiki_topics_output/`, `html_wiki_topics_stats.tsv`, `_chkpt/`)
6. View statistics: `cat workspace/store/wiki/join/html_wiki_topics_stats.tsv`
7. Run full join: `bin/spark_join_wiki_topics` (default 16 files per batch)
8. To restart from scratch: `bin/spark_join_wiki_topics --clean` then re-run
9. Adjust relevance filtering: `bin/spark_join_wiki_topics --relevantCategories "python,javascript,golang"`

**Building and testing index:**
1. Run `python3 -m indexer.build` with appropriate limits
2. Test queries: `python3 -m indexer.query --query "test"`
3. Compare IDF methods: `python3 -m indexer.compare`
4. Review reports in `reports/` directory

**Debugging issues:**
- Check logs in `workspace/logs/` (crawler, extractor, indexer)
- Check Spark logs in `logs/wiki_extract.jsonl` and `logs/wiki_join.jsonl`
- Inspect service stats in `workspace/state/service_stats.json`
- Use `--dry-run` to preview without side effects
- Use `--verbose` for debug-level logging (Python extractors)
- For Spark issues:
  - Check Docker container logs: `docker logs vinf-spark-extractor`
  - View Spark UI while running: http://localhost:4040
  - Check manifests in `runs/*/manifest.json`
- For Wikipedia extraction OOM errors:
  - Increase driver memory (most important): `SPARK_DRIVER_MEMORY=16g`
  - Increase partitions: `--partitions 1024`
  - Verify system has enough free RAM: `free -h` (Linux) or Activity Monitor (macOS)
  - Close other applications to free memory
- For slow Wikipedia extraction:
  - Verify using uncompressed XML (not .bz2)
  - Increase partitions for better parallelism
  - Check CPU usage is near 100%: `docker stats`
