# GitHub Crawl → Extract → Index Stack

This repository implements a three-stage pipeline for collecting public GitHub content, extracting structured metadata, and making the results searchable. Each stage can be run independently or chained end‑to‑end via the shared workspace defined in `config.yml`.

```
config.yml ──► python -m crawler
               │
               ├─ workspace/state/*.jsonl     frontier, fetched registries, stats
               ├─ workspace/store/html/       canonical HTML snapshots
               └─ workspace/metadata/*.jsonl  crawl outcomes + page typing
                  │
                  ▼
python -m extractor
               │
               ├─ workspace/store/text/       cleaned text mirrors
               └─ workspace/store/entities/   TSV entity catalogues
                  │
                  ▼
python -m indexer.build
               │
               ├─ workspace/store/index/*     docs.jsonl, postings.jsonl, manifest.json
               └─ reports/query/*             optional query reports
```

## Getting Started

### Requirements

- Python 3.10+ (async crawler relies on modern `asyncio` features)
- macOS/Linux (tested) with Git and a POSIX‑style shell
- Optional native libraries: `zstandard` (for compressed HTML storage) and `tiktoken` (token counts during indexing/query stats)

### Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

> Tip: install `black` and `isort` from `requirements-dev.txt` if present, or pip install them manually before formatting changes.

### Configuration Snapshot

Duplicate `config.yml` for new runs or environment overrides. The root keys are:

- `workspace`: absolute or relative path for runtime artefacts.
- `crawler`: crawl policy, scope, rate limits, storage layout, logging, proxy configuration.
- `extractor`: HTML input directory, output targets, feature toggles.
- `indexer`: build output selection, IDF defaults, query preferences and reporting.

All CLI entry points default to `config.yml`, but `--config /path/to/custom.yml` overrides that per command. Use CLI flags to override individual options (see sections below).

### Quick Pipeline Run

```bash
python -m crawler --config config.yml
python -m extractor --config config.yml --limit 50
python -m indexer.build --config config.yml
python -m indexer.query --config config.yml --query "async crawler"
```

Pause between stages as needed; each stage resumes off the workspace produced by the previous one.

## Repository Layout

| Path / Module | Description |
| ------------- | ----------- |
| `config.yml` | Unified configuration consumed by every stage. |
| `config_loader.py` | YAML loader + section helpers used by crawler/extractor/indexer configs. |
| `crawler/` | Frontier manager, fetch policy, HTTP client, and runtime service. |
| `extractor/` | Regex‑driven entity extraction and text cleaning pipeline. |
| `indexer/` | Inverted index builder, query engine, IDF utilities, reporting tools. |
| `tools/` | Operational helpers (`crawl_stats.py`). |
| `docs/` | Generated documentation and design notes. |
| `reports/` | Query and IDF comparison artefacts emitted by the indexer utilities. |
| `workspace/` | Default run output tree (mirrors layout in `config.yml`). |

### Module Reference

#### `crawler/`

- `__main__.py`: CLI bootstrap. Handles config load, logging setup, workspace preflight (`preflight_environment`). Only flag: `--config`.
- `config.py`: Dataclasses describing crawler configuration (`CrawlerScraperConfig`, `LimitsConfig`, `CapsConfig`, etc.). Handles YAML translation and validation (normalises user agents, seeds, proxy rules).
- `service.py`: `CrawlerScraperService` orchestrates the run—wires together frontier, policy, fetcher, link extractor, metadata writer, and runtime stats (HTML counts, acceptance rate, batch sleep cadence).
- `crawl_frontier.py`: File‑backed frontier queue. Maintains append‑only JSONL log plus auxiliary index directories for dedupe and resume support.
- `crawl_policy.py`: Enforces host allow/deny rules, regex scope filters, robots.txt compliance (with on‑disk cache + TTL). Uses `httpx` to fetch robots manifests.
- `unified_fetcher.py`: Async HTTP client around `httpx`. Handles user‑agent rotation, rate limiting, retries/backoff, proxy stubs, and optional Zstandard compression.
- `extractor.py`: Lightweight link extractor (regex based) producing canonicalised HTTP/HTTPS targets.
- `metadata_writer.py`: Append‑only JSONL writer for fetch metadata, ensures timestamps, persists per‑record extras.
- `url_tools.py`: URL normalisation helpers (`canonicalize`, `extract_repo_info`, GitHub URL predicates).

#### `extractor/`

- `__main__.py`: CLI dispatcher; parses a rich set of flags (see below) and wires config to `ExtractorPipeline`.
- `config.py`: Resolves extractor paths relative to `workspace`, maps YAML to `ExtractorConfig`, honours `outputs.text`, `outputs.entities`, and toggles like `enable_text`.
- `pipeline.py`: High‑level orchestrator; discovers HTML files, wires `HtmlFileProcessor`, writes TSV/text outputs, and logs run summary.
- `entity_main.py`: `HtmlFileProcessor` coordinates HTML reading, text extraction, entity detection, and output writing.
- `entity_extractors.py` / `regexes.py`: Regex catalogue for GitHub‑specific signals (stars, forks, README links, topics, emails, etc.). Update regex fixtures in lockstep with tests.
- `html_clean.py`: Deterministic HTML → text converter (boilerplate removal optional via `strip_boilerplate`).
- `io_utils.py`: File discovery and path derivation utilities (doc IDs, text mirrors).
- `outputs.py`: Writers for TSV entities and text mirrors (`EntitiesOutput`, `write_text`, `text_exists`).
- `data.py`: Run aggregation data classes (`ExtractionSummary`, per‑file stats).
- `ProposedArchitectureExtractor.md`: Design note describing the modernised pipeline plan.

#### `indexer/`

- `build.py`: Main CLI for generating inverted indexes. Implements streaming chunked builds via `IndexBuilder` and `IndexWriter`, supports optional `tiktoken` token counts. Flags documented below.
- `config.py`: Maps app config to `IndexerConfig`, resolves build/query paths, merges defaults.
- `ingest.py`: Streams `.txt` files to `DocumentRecord` structures, computes term frequencies and positions, and constructs per‑chunk vocabularies.
- `tokenize.py`: Deterministic tokeniser used during ingest and querying (alphanumeric + hex tokens, lowercased, skip single characters).
- `store.py`: Atomic writers for `docs.jsonl`, `postings.jsonl`, `manifest.json`, and streaming helpers for chunk merges.
- `idf.py`: IDF registry + calculator (supports `classic`, `smoothed`, `probabilistic`, `max` strategies).
- `search.py`: Search primitives (`IndexRepository`, `SearchEngine`) for loading index artefacts and executing ranked queries.
- `query.py`: CLI for ad‑hoc search; supports stored vs. computed IDF, reporting, and shows timing diagnostics.
- `compare.py`: CLI to compare rankings across multiple IDF strategies with TSV output.
- `idf.py` / `store.py` interplay: `IndexBuilder` computes IDF tables and persists them alongside postings for reuse during querying.

#### Utilities

- `tools/crawl_stats.py`: Summarises crawl outputs, optionally writes Markdown + CSV reports, and can sample extracted text for token statistics (requires `tiktoken`).
- `docs/generated/`: Destination for generated docs (e.g., crawl stats). Commit generated files intentionally.
- `reports/query/`: Default location for query Markdown reports created by `indexer.query` when `write_report` is enabled.

## Configuration Details (`config.yml`)

### Shared Keys

- `workspace`: Base directory for all artefacts. Relative paths are resolved under this root across modules.

### `crawler` Section

| Key | Summary |
| --- | ------- |
| `run_id` | Identifier stamped into logs/metadata. |
| `seeds` | Initial URLs. Empty entries are stripped; at least one required. |
| `user_agent` / `user_agents` + `user_agent_rotation_size` | Primary UA plus optional rotation list. |
| `accept_language`, `accept_encoding` | HTTP header hints. |
| `robots` | `user_agent` for robots fetch + `cache_ttl_sec`. |
| `scope` | Hosts whitelist, denied subdomains, allow/deny regex patterns, content type filters. |
| `limits` | Concurrency ceilings, request/sec, timeouts, retry/backoff plan. |
| `caps` | Depth and per‑repository guards. |
| `sleep` | Randomised pacing (per request and batch). |
| `storage` | File locations inside the workspace (frontier, fetched ledger, metadata, HTML root) plus compression and POSIX permissions. |
| `logs` | Log file path + log level. |
| `proxies` | Toggle, default URLs, pool, rotation strategy. |

### `extractor` Section

| Key | Summary |
| --- | ------- |
| `input_root` | Directory of HTML snapshots (default `store/html`). |
| `outputs.text`, `outputs.entities` | Text mirror directory and entity TSV target. |
| `enable_text`, `enable_entities` | Feature switches (can be overridden via CLI `--no-text` / `--no-entities`). |
| `limit`, `sample` | Caps on documents processed. |
| `force` | Reprocess even if outputs exist. |
| `dry_run` | List candidate files without processing. |
| `verbose` | Elevate logging to DEBUG. |

### `indexer` Section

| Key | Summary |
| --- | ------- |
| `default_idf` | Preferred IDF strategy when none specified. |
| `build.input_dir`, `build.output_dir` | Source text corpus and index output tree. |
| `build.limit` | Cap on number of documents ingested. |
| `build.dry_run` | Skip writing postings; useful for metadata counts. |
| `build.use_tokens` / `build.token_model` | Enable `tiktoken`-based token counts and pick the encoding. |
| `build.chunk_size` (computed in code) | Controls streaming batch size (default 1 000). |
| `query.index_dir` | Default index for queries (falls back to build output). |
| `query.top_k` | Default result count. |
| `query.idf_method` | `'manifest'` to reuse stored scores or explicit IDF name. |
| `query.show_path` | Toggle full path output. |
| `query.report_dir`, `query.write_report` | Report generation controls; path resolves under workspace unless absolute. |

## Stage 1 — Crawler

### Running

```bash
python -m crawler --config config.yml
```

The CLI prints the effective configuration summary, ensures workspace directories/files exist (`preflight_environment`), attaches logging, and hands control to `CrawlerScraperService`.

### Runtime Behaviour

- **Frontier management** (`CrawlFrontier`): Maintains append‑only queue (`state/frontier.jsonl`) and per‑URL markers in `frontier_index/`. Deduplicates fetched URLs via `state/fetched_urls.txt` + index.
- **Policy enforcement** (`CrawlPolicy`): Applies host rules, allow/deny regexes, depth caps (`caps.max_depth`), and robots.txt checks. Robots cache persists to `state/robots_cache.jsonl` with TTL refresh.
- **Fetching** (`UnifiedFetcher`): Async `httpx.AsyncClient` with keepalive, per‑request rate limiting, retry/backoff, user‑agent rotation, optional proxy selection, and HTML storage (SHA256 filename derived). Supports optional Zstandard compression (`storage.html_compress`).
- **Link extraction** (`LinkExtractor`): Regex parser that normalises HTTP(S) URLs, strips fragments, and filters unsupported schemes.
- **Metadata** (`UnifiedMetadataWriter`): Appends JSON records including status, latency, SHA256, stored path, referrer, depth, and optional extra metadata.
- **Stats** (`CrawlerScraperService`): Maintains counters (enqueued vs. denied URLs, fetch successes/errors, cap hits). Persists to `state/service_stats.json` every 60 s alongside frontier snapshots for resumability.
- **Sleep controls**: Tracks `sleep.batch_size` to insert randomised pauses between bursts, imitating human browsing.
- **Caps tracking**: Tracks per-repository page/issue/PR counts to enforce limits from config (`caps.per_repo_max_*`).

### Key Outputs

- `workspace/state/frontier.jsonl` & `_index/`: Queue persistence.
- `workspace/state/fetched_urls.txt` & `_index/`: Fetched dedupe ledger.
- `workspace/state/service_stats.json`: Runtime counters + storage metrics (`html_files_count`, `html_storage_bytes`).
- `workspace/metadata/crawl_metadata.jsonl`: One JSON line per fetch.
- `workspace/store/html/<sha>.html`: Stored HTML (optionally compressed) with original bytes preserved.
- `workspace/logs/crawler.log`: Run log adhering to configured level.

### Operational Notes

- Keep `user_agents` populated with contact information to respect GitHub guidelines.
- Start with a trimmed `seeds` list and inspect `service_stats.json` plus crawler logs before scaling.
- Use `tools/crawl_stats.py` for post‑run analysis (see Utilities section).
- Proxies are validated at config load; enabling them without URLs/pool raises early errors.

## Stage 2 — Extractor

### Running

```bash
python -m extractor \
  --config config.yml \
  --limit 200 \
  --entities-out workspace/store/entities/sample.tsv \
  --text-out workspace/store/text-sample \
  --force
```

CLI arguments (all optional unless stated):

| Flag | Effect |
| ---- | ------ |
| `--config` | Path to unified config. |
| `--in/--input` | Override HTML input directory. |
| `--text-out` | Override text output root. |
| `--entities-out` | Override entity TSV file. |
| `--limit` / `--sample` | Cap number of files processed. `--sample` aliases `--limit`. |
| `--force` | Rewrite text/entities even if they exist. |
| `--no-text`, `--no-entities` | Disable respective outputs regardless of config defaults. |
| `--dry-run` | List candidate files without processing. |
| `--verbose` | Enable DEBUG logging. |

### Runtime Behaviour

- `HtmlFileDiscovery` (from `io_utils`) enumerates HTML files deterministically (sorted) and enforces `limit`.
- `HtmlFileProcessor` reads HTML, sanitises it via `html_clean.html_to_text`, and writes cleaned text using `outputs.write_text`. Text mirrors mirror the HTML directory structure, aiding diffing and downstream ingest.
- `entity_extractors.extract_all_entities` detects GitHub‑specific signals (stars, forks, README/License detection, topics, emails, URLs) using precompiled regexes.
- `EntitiesOutput` writes TSV rows (`doc_id`, `entity_type`, `value`, `context_json`), ensuring directories exist and avoiding partial writes.
- `ExtractionSummary` aggregates stats for logging at the end (files processed, text/TSV outputs, counts per entity type).

### Outputs

- `workspace/store/text/<relative>.txt`: Clean text mirrors (if enabled).
- `workspace/store/entities/*.tsv`: Tab‑separated entity annotations.
- Logging via `logging` (respects `--verbose` or config).

### Customisation Tips

- Extend `regexes.py` when adding new entity types; accompany changes with fixture HTML under `tests_regex_samples/` (create the directory when adding tests).
- Use `--dry-run` to sanity check discovery patterns before running full extraction.
- Combine `--limit` with `--force` to reprocess a small sample during development without touching the full corpus.

## Stage 3 — Indexer

### Building the Index

```bash
python -m indexer.build \
  --config config.yml \
  --limit 10000 \
  --output workspace/store/index/dev \
  --use-tokens
```

CLI arguments:

| Flag | Effect |
| ---- | ------ |
| `--config` | Path to unified config file. |
| `--input` | Override input directory (defaults to `indexer.build.input_dir`). |
| `--output` | Override output directory (defaults to `indexer.build.output_dir`). |
| `--limit` | Limit number of documents ingested. |
| `--dry-run` | Skip writing outputs (still logs counts). |
| `--use-tokens` | Enable `tiktoken` token counting; requires package installed. |
| `--token-model` | Choose encoding (default `cl100k_base`). |
| `--chunk-size` | Override streaming chunk size (default 1 000). |
| `--idf` | Comma‑separated list of IDF methods to compute (defaults to all supported). |

Runtime flow:

- Streams `.txt` documents via `iter_document_records`, ensuring memory footprint stays low.
- Writes `docs.jsonl` incrementally (`append_docs`) and stores chunk postings in `output/partial/part_XXXX.jsonl`.
- Merges partial postings using heap‑based k‑way merge into `postings.jsonl`.
- Computes IDF tables through `IDFCalculator`, persisting them in each postings entry.
- Writes `manifest.json` summarising document/term counts and IDF methods included.

### Querying

```bash
python -m indexer.query \
  --config config.yml \
  --query "vector search" \
  --top 5 \
  --idf-method classic \
  --show-path
```

Behaviour:

- Loads index metadata via `IndexRepository`.
- Chooses IDF weighting: `manifest` (reuse stored scores) OR recompute via registry ensuring canonical names.
- `SearchEngine.search` tokenises the query with the same tokenizer used at ingest, scores results using configurable IDF + TF weighting, and returns `SearchResult` objects with matched term frequencies.
- Prints timing diagnostics (argument parse, config load, index load, query time).
- Generates Markdown reports when `query.write_report` is enabled; saved to `reports/query/<timestamp>.md`.

### Comparing IDF Strategies

```bash
python -m indexer.compare \
  --index workspace/store/index/default \
  --methods classic smoothed probabilistic \
  --queries "neural search" "async crawler" \
  --top 10 \
  --output reports/idf_compare.tsv
```

Produces console summaries plus TSV output capturing rankings per method and overlap/Jaccard metrics.

### Supporting Modules

- `idf.py`: Extend `CANONICAL_IDF_METHODS` to add new strategies (ensure tests cover them).
- `search.py`: Provides `IndexRepository`, `QueryVector`, and scoring logic; adapt here for alternative ranking schemes (BM25 variants, etc.).
- `store.py`: Handles atomic writes—extend when adding new artefacts to avoid partial files.
- `ingest.py`: Modify `iter_document_records` if you introduce new metadata (e.g., language tags).
- `tokenize.py`: Central place for tokenizer tweaks; keep behaviour deterministic for reproducible builds.

## Utilities & Reporting

- **`tools/crawl_stats.py`** – Summarise crawl outputs. Flags include:
  - `--workspace` / `--metadata-file` / `--service-stats-file` to control inputs.
  - `--markdown-output`, `--csv-output` for structured exports.
  - `--relevant-page-types` to focus on specific GitHub page classes.
  - `--text-root`, `--token-model`, `--token-limit` for token sampling.
- **`docs/generated/`** – Store generated Markdown/CSV from utilities. Keep under version control when useful for reports.
- **`reports/`** – Houses query run reports (`reports/query/`) and IDF comparisons (`reports/idf_comparison.tsv` by default).

## Workspace Layout Cheat Sheet

| Path | Produced By | Contents |
| ---- | ----------- | -------- |
| `workspace/state/frontier.jsonl` | crawler | Frontier queue (JSONL). |
| `workspace/state/frontier_index/` | crawler | On‑disk markers for queued URLs. |
| `workspace/state/fetched_urls.txt` | crawler | Fetched URL ledger. |
| `workspace/state/fetched_urls_index/` | crawler | Dedupe markers for fetched URLs. |
| `workspace/state/service_stats.json` | crawler | Run counters, HTML storage metrics, acceptance rate. |
| `workspace/state/robots_cache.jsonl` | crawler | Robots.txt cache with TTL metadata. |
| `workspace/metadata/crawl_metadata.jsonl` | crawler | Fetch metadata (status, bytes, latency, referrer, page type). |
| `workspace/store/html/` | crawler | HTML snapshots (SHA256 filenames, optional `.zst`). |
| `workspace/store/text/` | extractor | Plain-text mirrors of HTML files. |
| `workspace/store/entities/*.tsv` | extractor | Entity annotations. |
| `workspace/store/index/*` | indexer | Inverted index outputs (docs.jsonl, postings.jsonl, manifest.json, partial/). |
| `workspace/logs/crawler.log` | crawler | Run log captured via file handler. |
| `reports/query/*.md` | indexer.query | Markdown reports per query (when enabled). |

Clean up individual subdirectories instead of deleting the entire workspace when iterating; frontier/state files allow resumable crawls.

## Development Workflow

- **Formatting**: Run `black .` and `isort .` before committing changes to Python modules.
- **Static checks**: Optional extras (mypy/ruff) can be introduced; ensure they respect async crawler patterns.
- **Tests**: Add unit tests under `tests/` (create if absent) and run with `python3 -m unittest discover tests`. Regex regression fixtures belong under `tests_regex_samples/`.
- **Environments**: Keep API tokens or secrets out of `config.yml`. Use environment variables (e.g., `export GITHUB_TOKEN=...`) and load them inside modules if elevated access is required.

## Operational Guidance

- Ensure `crawler.user_agents` always include a contact email per GitHub crawling policy.
- Tune `caps` and `sleep` values for cautious rollouts on new target lists; monitor `service_stats.json` for growth and storage pressure.
- Review `workspace/logs/crawler.log` and `workspace/metadata/crawl_metadata.jsonl` after each run to confirm policy compliance and success rates.
- Before enlarging the crawl scope, run `tools/crawl_stats.py` to inspect status distributions and page type coverage.
- Prefer incremental index builds—`IndexBuilder` overwrites output files, so direct them to staged directories (`workspace/store/index/dev`) before promoting to production usage.
- When enabling proxies, populate either `http_url` / `https_url` or `pool` in `config.yml`; invalid configs raise early `ValueError`s during `CrawlerScraperConfig` construction.
- Optional compression (`storage.html_compress: true`) requires `zstandard`; install it via `pip install zstandard` if you plan to enable it.
- `indexer.query` report generation writes under `report_dir`; ensure the path exists or is creatable when running in restricted environments.

Happy crawling, extraction, and indexing! Use the modular design to iterate on each stage independently while keeping artefacts reproducible and traceable through the shared workspace hierarchy.

