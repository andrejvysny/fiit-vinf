# GitHub Crawl → Extract → Index + Wikipedia Enrichment Pipeline

This repository hosts a multi-stage data pipeline for analyzing public GitHub content with Wikipedia entity enrichment:

## Core Pipeline
1. **Crawler (`crawler/`)** – Policy-aware frontier crawler that fetches HTML and writes crawl metadata
2. **HTML Extractor (`extractor/` + `spark/jobs/html_extractor.py`)** – Regex-driven entity and text extraction from stored HTML (Python or Spark-based)
3. **Indexer (`indexer/`)** – Lightweight inverted index builders and query helpers for ad hoc search

## Wikipedia Extension Pipeline (NEW)
4. **Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)** – Streams and extracts structured data from 100GB+ Wikipedia XML dumps
5. **Entity-Wiki Join (`spark/jobs/join_html_wiki.py`)** – Joins HTML entities with Wikipedia canonical pages for entity resolution

The modules are loosely coupled through the shared `workspace/` directory, letting you resume or recompute individual stages independently.

---

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

All commands in this README assume the virtual environment is active.

---

## Architecture Overview

### Core Pipeline
```
config.yml ──► python -m crawler
              │
              ├─ HTML snapshots      → workspace/store/html/
              ├─ Crawl metadata      → workspace/metadata/crawl_metadata.jsonl
              ├─ Frontier state      → workspace/state/*
              └─ Logs & service stats→ workspace/logs/, workspace/state/service_stats.json
                 │
                 ▼
bin/spark_extract (or python -m extractor)
              │
              ├─ Raw text dumps      → workspace/store/text/
              └─ Entities TSV        → workspace/store/entities/entities.tsv
                 │
                 ▼
python -m indexer.build
              │
              ├─ docs.jsonl          (document table)
              ├─ postings.jsonl      (term → postings)
              └─ manifest.json       (index metadata)
```

### Wikipedia Extension Pipeline
```
wiki_dump/*.xml ──► bin/spark_wiki_extract
                   │
                   ├─ pages.tsv              → workspace/store/wiki/
                   ├─ categories.tsv
                   ├─ links.tsv
                   ├─ infobox.tsv
                   ├─ abstract.tsv
                   ├─ aliases.tsv
                   ├─ wiki_text_metadata.tsv (page → SHA256 mapping)
                   └─ text/                  → workspace/store/wiki/text/
                      └─ {SHA256}.txt        (deduplicated full article text)
                      │
                      ▼
entities.tsv + wiki/*.tsv ──► bin/spark_join_wiki
                              │
                              ├─ html_wiki.tsv     → workspace/store/join/
                              ├─ join_stats.json
                              └─ html_wiki_agg.tsv
```

Each module exposes its own CLI entry point so you can run the stages independently or automate them via scripts.

---

## Configuration (`config.yml`)

- `workspace` – shared root for runtime artefacts; module defaults resolve paths relative to this location.
- `crawler` – crawl metadata (`run_id`), seed URLs, user agents, robots/cache policy, scope allow/deny rules, rate limits, caps, sleep controls, storage layout, logging, and optional proxy settings.
- `extractor` – default input/output directories (nested under `outputs`), feature toggles (`enable_text`, `enable_entities`), and runtime behaviours such as `force`, `dry_run`, and `verbose`.
- `indexer` – build defaults (`default_idf`, `build` block with IO paths, limits, token settings) plus query-time preferences (`query` block with index path, `top_k`, `idf_method`, and `show_path`).

CLIs honour the configuration file by default; pass `--config /path/to/config.yml` to use an alternate profile or override individual options with command-line flags.

---

## Crawler (`crawler/`)

### Purpose

`CrawlerScraperService` orchestrates URL scheduling, fetch policy, HTTP retrieval, HTML storage, and metadata logging. It relies on a file-backed frontier (`CrawlFrontier`), robots-aware policy checks (`CrawlPolicy`), and storage helpers (`UnifiedFetcher`, `UnifiedMetadataWriter`).

### Running the crawler

```bash
python -m crawler --config config.yml
```

Key flags:

- `--config` – path to a YAML config (defaults to `config.yml`).

### Inputs

- `config.yml` (or custom path) – defines workspace location, seeds, user-agents, scope rules, rate limits, storage paths, and logging settings. See `crawler/config.py` for the full schema.

### Outputs

- `workspace/state/frontier.jsonl` – append-only frontier log with queued URLs.
- `workspace/state/fetched_urls.txt` – deduplication ledger of crawled URLs.
- `workspace/metadata/crawl_metadata.jsonl` – JSON lines with fetch outcomes (status codes, content hashes, timing, depth, referrers).
- `workspace/store/html/<sha256>.html` – content-addressed HTML snapshots.
- `workspace/state/service_stats.json` – periodic counters (fetch totals, acceptance rate, storage usage).
- `workspace/logs/crawler.log` – rotating run log (configured via `logs.log_file`).

### Operational Notes

- Frontier and fetched registries are backed by on-disk indexes to survive restarts; `CrawlerScraperService.start()` will resume previous state.
- Scope enforcement occurs before fetching: allowed hosts/patterns, depth limits (`caps.max_depth`), and per-repository quotas guard against crawl explosion.
- Rate limiting combines request spacing with periodic batch pauses (`sleep` section in `config.yml`).
- Robots.txt responses are cached to `workspace/state/robots_cache.jsonl` with TTL control.

---

## Extractor (`extractor/`)

### Purpose

Transforms stored HTML into analysis-ready artefacts:

- Raw text extraction with consistent HTML cleaning (`html_clean`).
- GitHub metadata detection (stars, forks, language stats) powered by regexes (`entity_extractors`, `regexes`).
- TSV writing and mirrored directory management (`io_utils`).

### Running the extractor

```bash
python -m extractor --config config.yml
```

Defaults come from the `extractor` section in `config.yml`; override with flags as needed. Useful options (`python -m extractor --help` shows the full list):

- `--limit N` / `--sample N` – cap processed files (sample is an alias).
- `--force` – overwrite existing text outputs instead of skipping.
- `--no-text`, `--no-entities` – disable individual outputs.
- `--dry-run` – list files without extracting.
- `--verbose` – promote logging to DEBUG.

### Inputs

- HTML tree under `workspace/store/html/` (mirrored structure produced by the crawler).

### Outputs (defaults unless disabled)

- `workspace/store/text/` – raw text (no boilerplate removal).
- `workspace/store/entities/entities.tsv` – tab-separated entities with offsets (`doc_id`, `type`, `value`, `offsets_json`).

### Implementation Notes

- Document IDs are derived from the HTML filename stem, preserving consistency across stages.
- Entity TSV writing is streaming and safe for large runs; headers are emitted once per invocation.
- Regex patterns live in `extractor/regexes.py`; add regression tests in `tests/` or `tests_regex_samples/` whenever patterns change.

---

## Indexer (`indexer/`)

### Purpose

Builds and queries an inverted index over the extractor’s text outputs. The module emphasises deterministic artefacts (JSON/JSONL) for easy diffing and grading.

Core components:

- `ingest.py` – walks `.txt` files, tokenises (`tokenize.py`), and produces in-memory `DocumentRecord`s.
- `build.py` – command-line interface for generating index artefacts with configurable IDF strategies.
- `search.py` – in-memory query engine with TF-IDF scoring.
- `query.py` – CLI wrapper around `search.py` for quick lookups.
- `compare.py` – generates Markdown reports comparing IDF methods.

### Building an index

```bash
python3 -m indexer.build --config config.yml
```

Defaults come from `indexer.build` in `config.yml`; override with flags such as: 

- `--limit` – trim document count for smoke tests.
- `--dry-run` – compute stats without writing files.
- `--use-tokens` / `--token-model` – record tiktoken counts (requires optional dependency).
- `--idf-method` – force a specific IDF weighting (overrides `indexer.default_idf`).

### Querying the index

```bash
python3 -m indexer.query --config config.yml --query "async crawler"
```

Defaults come from `indexer.query` in `config.yml`. Useful overrides:

- `--idf-method` – pick `manifest` or any supported method (`classic`, `smoothed`, `probabilistic`, `max`).
- `--top` – adjust result count.
- `--show-path` – display absolute document paths.

### Comparing IDF strategies

```bash
python3 -m indexer.compare \
  --index workspace/store/index/dev \
  --query "search ranking" \
  --top 5 \
  --output reports/idf_comparison.tsv
```

Emits a TSV report under `reports/` and prints a console recap of pairwise overlaps.

### Outputs

When `indexer.build` runs (non dry-run):

- `docs.jsonl` – per-document metadata (`doc_id`, `path`, `title`, `length`, optional token counts).
- `postings.jsonl` – vocabulary with term frequency lists and stored IDF values for each supported method (`classic`, `smoothed`, `probabilistic`, `max`).
- `manifest.json` – summary of corpus size, term count, and default IDF choice.

---

## Wikipedia Extractor (`spark/jobs/wiki_extractor.py`)

### Purpose

Extracts structured data from massive Wikipedia XML dumps (100GB+) without running out of memory. Uses Spark's streaming architecture with NO caching to process ~7 million pages efficiently.

### Running Wikipedia extraction

```bash
# Quick test (50-100 pages, ~20 seconds)
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# Full extraction (~2-3 hours on 32GB RAM system)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512
```

Key flags:
- `--wiki-in DIR` – Input directory with Wikipedia dumps (default: `wiki_dump`)
- `--out DIR` – Output directory for TSV files (default: `workspace/store/wiki`)
- `--wiki-max-pages N` – Limit pages for testing (default: all pages)
- `--partitions N` – Number of Spark partitions (default: 256, auto-scales for large files)
- `--log FILE` – Log file path (default: `logs/wiki_extract.jsonl`)

### Inputs

- Wikipedia XML dump (uncompressed, ~104GB): `wiki_dump/enwiki-20250901-pages-articles-multistream.xml`

### Outputs (7 TSV files + full text directory)

1. `pages.tsv` – Page metadata (page_id, title, norm_title, namespace, redirect_to, timestamp)
2. `categories.tsv` – Page-category mappings (page_id, category, norm_category)
3. `links.tsv` – Internal wiki links (page_id, link_title, norm_link_title)
4. `infobox.tsv` – Infobox key-value pairs (page_id, key, value)
5. `abstract.tsv` – First paragraph text (page_id, abstract_text)
6. `aliases.tsv` – Redirect mappings for title normalization (alias_norm_title, canonical_norm_title)
7. `wiki_text_metadata.tsv` – **NEW**: Metadata linking pages to full article text (page_id, title, content_sha256, content_length, timestamp)
8. `text/` – **NEW**: Full article text files with SHA256-based deduplication (`{SHA256}.txt`)

### Implementation Notes

- **Streaming architecture**: Uses `mapPartitions` with buffer limits (max 50K lines/page) to prevent OOM
- **NO caching**: Never caches DataFrames to avoid loading 100GB+ into memory
- **Auto-scaling partitions**: 256+ partitions for files > 50GB for optimal parallelism
- **Memory requirements**: Minimum 16GB RAM (12g driver, 6g executor), recommended 32GB for better performance
- **Title normalization**: Removes parenthetical suffixes, lowercases, ASCII-folds, collapses punctuation
- **Full text extraction**: Cleans wikitext markup (templates, links, refs) and stores deduplicated plaintext using SHA256 content addressing
- **Deduplication**: Identical article content (after cleaning) stored only once, metadata TSV maps pages to text files
- **Configurable**: Use `--no-text` to disable full text extraction if only structured data is needed
- See `WIKI_EXTRACTION.md` for detailed technical specification and troubleshooting

---

## Entity-Wikipedia Join (`spark/jobs/join_html_wiki.py`)

### Purpose

Joins HTML entities (extracted from GitHub pages) with Wikipedia canonical pages to enrich entities with authoritative knowledge. Handles aliases, calculates confidence scores, and resolves ambiguous matches.

### Running the join

```bash
# Test with sample (10K entities, ~2-3 minutes)
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join \
  --entities-max-rows 10000

# Full join (all entities, ~5-10 minutes)
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join
```

Key flags:
- `--entities FILE` – Path to entities.tsv from HTML extraction (required)
- `--wiki DIR` – Directory with Wikipedia TSV files (required)
- `--out DIR` – Output directory (default: `workspace/store/join`)
- `--entities-max-rows N` – Limit entity rows for testing
- `--partitions N` – Number of Spark partitions (default: 64)

### Inputs

- `workspace/store/entities/entities.tsv` – HTML entities from extractor
- `workspace/store/wiki/*.tsv` – Wikipedia structured data (6 files)

### Outputs (3 files)

1. `html_wiki.tsv` – Main join results (doc_id, entity_type, entity_value, norm_value, wiki_page_id, wiki_title, join_key, confidence)
2. `join_stats.json` – Overall statistics and match rates by entity type
3. `html_wiki_agg.tsv` – Per-document aggregates (doc_id, entity_type, total, matched)

### Supported Entity Types

- **LANG_STATS** – Programming languages (60-90% match rate)
- **LICENSE** – Software licenses (80-95% match rate)
- **TOPICS** – GitHub topics (30-50% match rate, comma-separated values are exploded) - *See also: Topics Streaming JOIN below*
- **README** – Keywords from README text (10-30% match rate)
- **STAR_COUNT, FORK_COUNT, URL** – Not joined (metadata/filtering only)

### Implementation Notes

- **Canonical mapping**: Builds normalized title → page mapping with alias resolution
- **Confidence scoring**: 0.0-1.0 scores based on match type, case sensitivity, and category hints
- **Topic explosion**: TOPICS values like "python,django,postgresql" are split and joined separately
- **Memory requirements**: 8-16GB RAM (6g driver, 3g executor)
- See `JOIN_PLAN_WIKI.md` for detailed join strategy and validation commands

---

## Topics Streaming JOIN (`spark/jobs/join_html_wiki_topics.py`) **NEW**

### Purpose

**Specialized streaming join** for GitHub TOPICS → Wikipedia articles with relevance filtering. Unlike the batch join above, this uses Spark Structured Streaming for incremental processing of large entity datasets with category-based and abstract-based relevance filters.

### Running the streaming join

```bash
# Basic run with default settings
bin/spark_join_wiki_topics

# Custom configuration
bin/spark_join_wiki_topics \
  --maxFilesPerTrigger 16 \
  --relevantCategories "programming,software,computer,library,framework" \
  --absHit true
```

Key flags:
- `--entities FILE` – Path to entities TSV (default: `workspace/store/spark/entities/entities.tsv`)
- `--wiki DIR` – Wiki dimensions directory (default: `workspace/store/wiki`)
- `--out DIR` – Output directory (default: `workspace/store/wiki/join`)
- `--checkpoint DIR` – Streaming checkpoint (default: `workspace/store/wiki/join/_chkpt/topics`)
- `--maxFilesPerTrigger N` – Bounded memory control (default: 16)
- `--relevantCategories` – Comma-separated relevance keywords (default: programming,software,computer,library,framework,license)
- `--absHit BOOL` – Enable abstract text matching (default: true)

### Inputs

- HTML entities TSV (streaming source) – TOPICS field only
- Wikipedia dimensions (batch/static):
  - `pages.tsv` (ns==0, main namespace)
  - `aliases.tsv` (redirect resolution)
  - `categories.tsv` (relevance filtering)
  - `abstract.tsv` (text-based matching)

### Outputs

1. `html_wiki_topics_output/` – Spark CSV parts with joined data (TSV format)
   - Columns: doc_id, entity_type, entity_value, norm_value, wiki_page_id, wiki_title, join_method, confidence, categories_json, abstract_text
2. `html_wiki_topics_stats.tsv` – Per-batch statistics with timestamps

### Join Logic

**Multi-hop title resolution:**
1. Normalize topic text (lowercase, ASCII-fold, punct collapse)
2. Resolve aliases: topic → canonical_title (via Wikipedia redirects)
3. Match canonical_title → pages.norm_title
4. Enrich with categories and abstracts
5. Filter for relevance:
   - Category contains keywords (programming, software, etc.) OR
   - Abstract contains normalized topic

**Confidence scoring:**
- `exact+cat`: Direct title match + relevant category
- `alias+cat`: Alias resolution + relevant category
- `exact+abs`: Direct match + abstract contains topic
- `alias+abs`: Alias resolution + abstract contains topic

### Implementation Notes

- **Structured Streaming**: Incremental processing with checkpoint-based state
- **Bounded memory**: maxFilesPerTrigger prevents memory overflow
- **No broadcasts**: Static dimensions joined without broadcasting large tables
- **Relevance filtering**: Only technology-related Wikipedia pages matched
- **Per-batch stats**: foreachBatch tracks join rates and unique page counts
- **Java compatibility**: Requires Java 11 or 17 (Java 24 not supported by Spark 4.0.1)
- See `SPARK_JOIN_IMPLEMENTATION.md` for detailed architecture and troubleshooting

---

## Workspace Layout Cheatsheet

| Path | Producer | Description |
| ---- | -------- | ----------- |
| `workspace/state/frontier.jsonl` | crawler | Append-only queue of pending URLs |
| `workspace/state/fetched_urls.txt` | crawler | De-dup ledger of crawled URLs |
| `workspace/state/service_stats.json` | crawler | Runtime counters and storage metrics |
| `workspace/store/html/` | crawler | Canonical HTML snapshots (SHA256 filenames) |
| `workspace/metadata/crawl_metadata.jsonl` | crawler | JSON lines with fetch metadata |
| `workspace/store/text/` | extractor | Raw text dumps mirroring HTML tree |
| `workspace/store/entities/entities.tsv` | extractor | Tab-separated entity annotations |
| `workspace/store/index/*` | indexer | Inverted index artefacts |
| `workspace/store/wiki/*.tsv` | wiki_extractor | Wikipedia structured data (7 TSV files) |
| `workspace/store/wiki/text/*.txt` | wiki_extractor | **NEW**: Full article text (SHA256-named, deduplicated) |
| `workspace/store/join/*.tsv` | join_html_wiki | Entity-Wikipedia batch join results |
| `workspace/store/wiki/join/html_wiki_topics_output/` | join_html_wiki_topics | **NEW**: Topics streaming join results |
| `workspace/store/wiki/join/_chkpt/` | join_html_wiki_topics | **NEW**: Streaming checkpoints |
| `workspace/logs/crawler.log` | crawler | Rolling crawl logs |
| `logs/wiki_extract.jsonl` | wiki_extractor | Wikipedia extraction structured logs |
| `logs/wiki_join.jsonl` | join_html_wiki | Join pipeline structured logs |
| `runs/*/manifest.json` | Spark jobs | Run metadata with checksums and statistics |

Clean up runtime artefacts (e.g. purge specific directories under `workspace/`) manually when re-running large experiments; avoid force-overwriting existing data unless necessary.

---

## Testing & Validation

Unit tests live in `tests/` and exercise crawler services plus regex extraction helpers.

```bash
python3 -m unittest tests.test_crawler_service tests.test_link_extractor
python3 -m unittest discover tests
```

If you update regexes or scope rules, add fixtures under `tests_regex_samples/` and corresponding test cases.

---

## Operational Tips

### Core Pipeline
- Keep contact information in `config.yml` user-agents to comply with GitHub's crawling guidelines.
- Start with a trimmed `config.yml` seeds list and inspect `workspace/logs/crawler.log` plus `workspace/state/service_stats.json` before scaling up.
- Monitor storage consumption via the service stats JSON (`html_storage_bytes` and `html_files_count` fields).
- Use `tools/` helpers (e.g. `python3 tools/crawl_stats.py`) to inspect run-time metrics when available.
- When adjusting scope patterns, validate against fixture HTML and run targeted crawls to ensure the frontier grows as expected.

### Wikipedia Pipeline
- **Always test first**: Run `--wiki-max-pages 100` before attempting full extraction to verify memory settings
- **Memory requirements**: Minimum 16GB RAM for full Wikipedia extraction, recommended 32GB+
- **Storage needs**: ~150GB total (104GB input dump + 10-15GB outputs + temp space)
- **Expected duration**: 2-3 hours for full 7M page extraction on 32GB system
- **Monitor progress**: Spark UI at http://localhost:4040 while job is running
- **Check outputs**: Verify all 6 TSV files created with `ls -lh workspace/store/wiki/`
- **Validate join results**: Review `workspace/store/join/join_stats.json` for match rates by entity type
- **Troubleshooting OOM**: Increase driver memory (`SPARK_DRIVER_MEMORY=16g`) and partitions (`--partitions 1024`)

### Complete Pipeline Workflow

To run the entire pipeline from scratch:

```bash
# 1. Crawl GitHub (if not already done)
python -m crawler --config config.yml

# 2. Extract HTML entities (Spark recommended)
export SPARK_DRIVER_MEMORY=6g
export SPARK_EXECUTOR_MEMORY=4g
bin/spark_extract --partitions 256 --force

# 3. Extract Wikipedia data (test first!)
bin/spark_wiki_extract --wiki-max-pages 100 --partitions 8

# 4. Run full Wikipedia extraction (requires 16-32GB RAM, ~2-3 hours)
SPARK_DRIVER_MEMORY=12g SPARK_EXECUTOR_MEMORY=6g \
bin/spark_wiki_extract --partitions 512

# 5. Join entities with Wikipedia
bin/spark_join_wiki \
  --entities workspace/store/entities/entities.tsv \
  --wiki workspace/store/wiki \
  --out workspace/store/join

# 6. Build search index
python3 -m indexer.build --config config.yml

# 7. Query results
python3 -m indexer.query --config config.yml --query "your search terms"

# 8. Verify Wikipedia join results
cat workspace/store/join/join_stats.json | jq .
```

**Total estimated time**: ~3-4 hours (depending on system specs)

---

## Additional Documentation

- **WIKI_EXTRACTION.md** – Comprehensive Wikipedia extraction technical specification
- **JOIN_PLAN_WIKI.md** – Entity-Wikipedia join strategy and validation guide
- **RUN.md** – Detailed run instructions for all three Spark jobs
- **CLAUDE.md** – Project instructions for Claude Code AI assistant

---

Happy crawling and extracting! The individual modules can be run end-to-end or independently, giving you flexibility to iterate on extraction rules, indexing strategies, or crawl policies without reprocessing everything.