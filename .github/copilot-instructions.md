## Overview

This repo hosts a two-part GitHub-focused data pipeline:
- `crawler/` discovers allowed URLs, manages the frontier/dedup/robots/policy stack, and appends discoveries as JSONL segments under `workspace/spool/discoveries/`.
- `scraper/` tails those discovery files, fetches HTML, writes blobs to a content-addressed store, and records metadata + metrics in CSV/LMDB.

`./vi-scrape` is the convenience CLI for configuring the workspace, running both services together, and resetting state.

## Workspace layout (relative to repo root)
- `workspace/spool/discoveries/discoveries-<ts>.jsonl` — newline-delimited discoveries written by `crawler/io/discoveries.py` (fsync on every write).
- `workspace/state/spool_bookmark.json` — `{"file": <name>|null, "offset": <int>}` checkpoint consumed by `scraper/io/spool.py`.
- `workspace/state/frontier.lmdb/` — LMDB priority queue managed by `crawler/frontier/frontier.py`.
- `workspace/state/seen/aa/bb/<sha>.seen` — sharded dedup touch files from `crawler/frontier/dedup.py`.
- `workspace/state/page_index.lmdb/` — scraper index rows `{url, sha256, stored_path, etag, last_modified, timestamp}`.
- `workspace/store/html/<sha[:2]>/<sha[2:4]>/<sha>.html[.zst]` — blobs stored via `scraper/io/storage.py`; optional zstd compression.
- `workspace/logs/pages-*.csv` & `workspace/logs/scraper_metrics-*.csv` — rows from `scraper/io/csv_writer.py` and metrics flushes from `scraper/io/metrics.py`.
- `workspace/logs/crawler_metrics.csv` & `workspace/trajectory/edges.csv` (if enabled) — crawler metrics/graph output.

Configs (`config.yaml`, `config_run.yaml`, `scraper_config.yaml`) define all paths relative to `workspace/`. Ensure new code respects those conventions.

## Running commands
- Install dependencies: create/activate a Python 3.11+ venv, then `pip install -r requirements.txt`.
- Quick start: `./vi-scrape run` (or add overrides: `./vi-scrape run --config config_run.yaml --scraper-config scraper_config.yaml --seeds seeds.txt`). Wrapper ensures directory scaffolding, starts crawler in the background, and runs scraper in the foreground. `Ctrl+C` triggers graceful shutdown and terminates the crawler.
- `./vi-scrape configure` seeds the workspace directories and drops a default `seeds.txt`. `./vi-scrape reset` wipes spool/store/state/logs after interactive confirmation.
- Scraper CLI (explicit config required):
  - `python -m scraper run --config scraper_config.yaml` — auto-resume from bookmark if present.
  - `python -m scraper resume --config scraper_config.yaml` — strict resume; errors if bookmark missing.
  - `python -m scraper replay --config scraper_config.yaml` — rebuild CSV from stored HTML (offline).
  - `python -m scraper status --config scraper_config.yaml` — snapshot (bookmark, spool size, store counts, control flags).
- Crawler CLI (`--config` precedes subcommand):
  - `python -m crawler --config config.yaml run --seeds seeds.txt`
  - `python -m crawler --config config.yaml validate-policy --url <URL>`
  - `python -m crawler --config config.yaml status`

## Contracts & invariants
- **Strict policy** — `crawler/policy.py` deny-by-default rules must stay conservative. `test_policy.py` encodes the allowlist/denylist contracts; update tests/configs together.
- **Robots compliance** — `crawler/robots_cache.py` consults GitHub robots before enqueuing. User agents in configs must include operator contact info to pass validation and external policies.
- **Discovery schema** — JSON keys: `url`, `page_type`, `depth`, `referrer`, `metadata`. Preserve JSONL formatting and bookmark semantics (byte offsets) to keep `scraper/io/spool.py` resilient.
- **Backpressure & control flags** — `scraper/util/bytes.dir_size_gb` enforces `spool.max_backlog_gb`; `scraper/control/flags.py` watches `workspace/pause.scraper` / `workspace/stop.scraper`. Any loop changes must continue honoring these checks.
- **Storage/index coupling** — `scraper/io/storage.py` handles hashing + (optional) compression and returns paths consumed by `scraper/index/page_index.py`. Maintain the two-level shard layout and update replay logic/tests if storage format changes.
- **Rate limiting** — request pacing in both services (`crawler/net/fetcher.py`, `scraper/util/limits.py`, `scraper/net/fetcher.py`) relies on configured concurrency/RPS and exponential backoff. Adjust limits only with matching config/test updates.
- **Signals** — `scraper/util/signals.py` wires SIGHUP reload and SIGUSR1 status dumps; keep hooks intact during refactors.

## Useful code entry points
- `scraper/run.py` — service orchestration (spool iteration, storage, metrics, cleanup, replay).
- `scraper/net/fetcher.py` — HTTP client with conditional requests, proxy integration, retries, and HTML filtering.
- `crawler/run.py` — main crawler loop (frontier pop, fetch, classify, caps, discovery writes).
- `crawler/frontier/frontier.py` & `crawler/frontier/dedup.py` — LMDB-backed frontier and sharded dedup store.
- Docs: `docs/CRAWLER.md`, `docs/SCRAPER.md`, `SCRAPER_USER_GUIDE.md` for architecture, ops notes, and CLI walkthroughs.

## Tests & verification
- Top-level `test_*.py` run with `pytest -q`.
- Focused suites:
  - `test_policy.py` — allow/deny expectations.
  - `test_spool.py` — bookmark + ordering guarantees.
  - `test_scraper_integration.py` — scraper end-to-end fixture.
  - `test_fetcher.py`, `test_fetcher_simple.py` — fetcher behaviour and rate limiting.
  - `test_index.py`, `test_storage.py` — persistence contracts.
- Network/storage changes should ship with updated fixtures/tests and docs (`SCRAPER_USER_GUIDE.md`). Avoid live GitHub calls in tests; rely on local inputs.

## Editing guidance for AI assistants
- Keep discoveries JSONL, bookmark JSON, LMDB layouts, and CSV headers unchanged unless requirements shift — call out impacts when proposing format changes.
- Default to ASCII for new code/comments. Leave unrelated files untouched; never revert user edits unless specifically asked.
- When altering runtime loops, re-verify signal handling, bookmark persistence, control flags, and backpressure checks.
- Coordinate any policy or scope changes with matching config/test updates; stay conservative on allowed patterns/hosts.
- Ask for clarification if operational expectations are unclear — do not guess.

Need deeper CLI examples, data schemas, or troubleshooting tips? Let the user know which section to expand and this file can grow accordingly.
