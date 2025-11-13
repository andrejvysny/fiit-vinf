# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a three-stage data pipeline for analyzing public GitHub content:
1. **Crawler** – Fetches HTML and writes crawl metadata
2. **Extractor** – Extracts entities and text from stored HTML
3. **Indexer** – Builds inverted indexes and enables search

The pipeline is loosely coupled through the `workspace/` directory, allowing independent execution or resumption of each stage.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

All commands assume the virtual environment is active.

## Core Commands

### Running the Pipeline

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

### Testing

```bash
# Run all tests
python -m unittest discover tests

# Run specific test files
python -m unittest tests.test_regexes
python -m unittest tests.test_link_extractor
```

### Useful Flags

**Extractor:**
- `--limit N` – Process only N files (for smoke tests)
- `--force` – Overwrite existing outputs
- `--dry-run` – List files without processing
- `--verbose` – Enable debug logging
- `--no-text` / `--no-entities` – Disable specific outputs

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

### Key Components

**Crawler (`crawler/`)**:
- `service.py` – Main `CrawlerScraperService` orchestrates crawling
- `crawl_frontier.py` – File-backed URL queue with deduplication
- `crawl_policy.py` – Robots.txt-aware policy enforcement
- `unified_fetcher.py` – HTTP retrieval with retry logic
- `metadata_writer.py` – JSONL metadata logging

**Extractor (`extractor/`)**:
- `pipeline.py` – Main `ExtractorPipeline` orchestrator
- `entity_extractors.py` – GitHub metadata extraction (stars, forks, languages)
- `regexes.py` – Regex patterns for entity detection
- `html_clean.py` – HTML cleaning and text extraction
- Spark version in `spark/jobs/html_extractor.py` for parallel processing

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
| `workspace/logs/` | all | Module logs |

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
- Spark extractor runs in Docker with local volume mounts
- Uses `wholeTextFiles` for distributed text loading
- Tokenization UDF enables DataFrame-based processing
- Repartitions by term before aggregation to avoid skew
- Writes consolidated TSV files (no Spark part files)
- Generates manifests with SHA-1 checksums for reproducibility

## Common Operational Patterns

**Starting a new crawl:**
1. Update `config.yml` seeds and scope rules
2. Clear or backup `workspace/state/` if starting fresh
3. Run crawler and monitor `workspace/logs/crawler.log`
4. Check `workspace/state/service_stats.json` for metrics

**Iterating on extraction rules:**
1. Modify patterns in `extractor/regexes.py`
2. Add test cases in `tests/test_regexes.py`
3. Run `python -m unittest tests.test_regexes`
4. Test on small sample: `python -m extractor --limit 100`
5. Use `--force` to overwrite existing outputs

**Building and testing index:**
1. Run `python3 -m indexer.build` with appropriate limits
2. Test queries: `python3 -m indexer.query --query "test"`
3. Compare IDF methods: `python3 -m indexer.compare`
4. Review reports in `reports/` directory

**Debugging issues:**
- Check logs in `workspace/logs/`
- Inspect service stats in `workspace/state/service_stats.json`
- Use `--dry-run` to preview without side effects
- Use `--verbose` for debug-level logging
- For Spark issues, check Docker container logs
