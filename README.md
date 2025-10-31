# GitHub Crawl → Extract → Index Pipeline

This repository hosts a three-stage data pipeline tailored for analysing public GitHub content. It consists of:

1. **Crawler (`crawler/`)** – policy-aware frontier crawler that fetches HTML and writes crawl metadata.
2. **Extractor (`extractor/`)** – regex-driven entity and text extraction over stored HTML.
3. **Indexer (`indexer/`)** – lightweight inverted index builders and query helpers for ad hoc search.

The modules are loosely coupled through the shared `workspace/` directory, letting you resume or recompute individual stages.

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

```
config.yaml ──► python -m crawler
              │
              ├─ HTML snapshots      → workspace/store/html/
              ├─ Crawl metadata      → workspace/metadata/crawl_metadata.jsonl
              ├─ Frontier state      → workspace/state/*
              └─ Logs & service stats→ workspace/logs/, workspace/state/service_stats.json
                 │
                 ▼
python -m extractor
              │
              ├─ Raw text dumps      → workspace/store/text/
              ├─ Preprocessed text   → workspace/store/text-preprocessed/
              ├─ README extracts     → workspace/store/readme/
              └─ Entities TSV        → workspace/store/entities/entities.tsv
                 │
                 ▼
python -m indexer.build
              │
              ├─ docs.jsonl          (document table)
              ├─ postings.jsonl      (term → postings)
              └─ manifest.json       (index metadata)
```

Each module exposes its own CLI entry point so you can run the stages independently or automate them via scripts in `tools/`.

---

## Crawler (`crawler/`)

### Purpose

`CrawlerScraperService` orchestrates URL scheduling, fetch policy, HTTP retrieval, HTML storage, and metadata logging. It relies on a file-backed frontier (`CrawlFrontier`), robots-aware policy checks (`CrawlPolicy`), and storage helpers (`UnifiedFetcher`, `UnifiedMetadataWriter`).

### Running the crawler

```bash
python -m crawler --config config.yaml
```

Key flags:

- `--config` – path to a YAML config (defaults to `config.yaml`).

### Inputs

- `config.yaml` (or custom path) – defines workspace location, seeds, user-agents, scope rules, rate limits, storage paths, and logging settings. See `crawler/config.py` for the full schema.

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
- Rate limiting combines request spacing with periodic batch pauses (`sleep` section in `config.yaml`).
- Robots.txt responses are cached to `workspace/state/robots_cache.jsonl` with TTL control.

---

## Extractor (`extractor/`)

### Purpose

Transforms stored HTML into analysis-ready artefacts using regex-only tooling:

- Boilerplate stripping and text normalisation (`html_clean`).
- Entity recognition for GitHub-specific metadata, README sections, licenses, tags, import statements, issues, versions, emails, and URLs (`entity_extractors`, `regexes`).
- TSV writing and mirrored directory management (`io_utils`).

### Running the extractor

```bash
python -m extractor \
  --in workspace/store/html \
  --text-out workspace/store/text \
  --entities-out workspace/store/entities/entities.tsv
```

Useful options (`python -m extractor --help` shows the full list):

- `--limit N` / `--sample N` – cap processed files (sample is an alias).
- `--force` – overwrite existing text/preprocessed outputs instead of skipping.
- `--no-text`, `--no-preproc`, `--no-entities`, `--no-readme` – disable individual outputs.
- `--dry-run` – list files without extracting.
- `--verbose` – promote logging to DEBUG.

### Inputs

- HTML tree under `workspace/store/html/` (mirrored structure produced by the crawler).

### Outputs (defaults unless disabled)

- `workspace/store/text/` – raw text (no boilerplate removal).
- `workspace/store/text-preprocessed/` – cleaned text (boilerplate removed).
- `workspace/store/readme/` – README-only snippets.
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
python3 -m indexer.build \
  --input workspace/store/text \
  --output workspace/store/index/dev \
  --idf-method log
```

Selected flags:

- `--limit` – trim document count for smoke tests.
- `--dry-run` – compute stats without writing files.
- `--use-tokens` / `--token-model` – record tiktoken counts (requires optional dependency).

### Querying the index

```bash
python3 -m indexer.query \
  --index workspace/store/index/dev \
  --query "async crawler" \
  --top 5 \
  --idf-method manifest \
  --show-path
```

- `--idf-method manifest` uses the stored default from `manifest.json`.
- Specify `log` or `rsj` to recompute weights on the fly.

### Comparing IDF strategies

```bash
python3 -m indexer.compare \
  --index workspace/store/index/dev \
  --query "search ranking" \
  --top 5 \
  --output docs/generated/index_comparison.md
```

### Outputs

When `indexer.build` runs (non dry-run):

- `docs.jsonl` – per-document metadata (`doc_id`, `path`, `title`, `length`, optional token counts).
- `postings.jsonl` – vocabulary with term frequency lists and stored IDF values for each supported method (`log`, `rsj`).
- `manifest.json` – summary of corpus size, term count, and default IDF choice.

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
| `workspace/store/text-preprocessed/` | extractor | Boilerplate-stripped text |
| `workspace/store/readme/` | extractor | README-only text extracts |
| `workspace/store/entities/entities.tsv` | extractor | Tab-separated entity annotations |
| `workspace/store/index/*` | indexer | Inverted index artefacts |
| `workspace/logs/crawler.log` | crawler | Rolling crawl logs |

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

- Keep contact information in `config.yaml` user-agents to comply with GitHub’s crawling guidelines.
- Start with a trimmed `config.yaml` seeds list and inspect `workspace/logs/crawler.log` plus `workspace/state/service_stats.json` before scaling up.
- Monitor storage consumption via the service stats JSON (`html_storage_bytes` and `html_files_count` fields).
- Use `tools/` helpers (e.g. `python3 tools/crawl_stats.py`) to inspect run-time metrics when available.
- When adjusting scope patterns, validate against fixture HTML and run targeted crawls to ensure the frontier grows as expected.

---

Happy crawling! The individual modules can be run end-to-end or independently, giving you flexibility to iterate on extraction rules, indexing strategies, or crawl policies without reprocessing everything.
