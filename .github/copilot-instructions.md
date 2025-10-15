# Copilot Working Notes
## Architecture
- Entry point `main.py` loads `CrawlerScraperConfig` from YAML and runs `CrawlerScraperService`; no other runners exist.
- `CrawlerScraperService` orchestrates the crawl loop: seed frontier → pop URL → `UnifiedFetcher.fetch_and_store` → `LinkExtractor.extract` → enqueue via `CrawlPolicy.gate` → persist via `UnifiedMetadataWriter`.
- `CrawlFrontier` stores queue state in `workspace/state/frontier.jsonl` and fetched URLs in `workspace/state/fetched_urls.txt`; it enforces breadth-first order and dedup across restarts.
- Policy decisions live in `crawler/crawl_policy.py`; URLs must match allowlist regexes, avoid deny patterns, and pass robots.txt (cached in `workspace/state/robots_cache.jsonl`).
- Fetching and storage are unified: `crawler/net/unified_fetcher.py` writes content-addressed HTML files under `workspace/store/html/aa/bb/<sha>.html` and returns metadata for the same request.
- Unified metadata is appended to `workspace/metadata/crawl_metadata.jsonl`; legacy discovery/trajectory writers intentionally raise `ImportError`—do not revive the old producer/consumer flow.

## Configuration & Conventions
- Treat YAML config (`config.yaml`) as source of truth; any new knobs require updating `CrawlerScraperConfig` dataclasses plus validation in `__post_init__`.
- Keep URL handling centralized in `crawler/url_tools.py`; always canonicalize before dedup/policy checks to preserve resume semantics.
- Caps enforcement (`_check_caps`) tracks per repo pages/issues/PRs—if adding new page types, adjust the counters and config caps together.
- Policy regexes are deny-by-default; prefer updating the allowlist rather than bypassing the gate downstream.
- HTML processing expects UTF-8; `UnifiedFetcher` currently decodes with `errors='ignore'`, so downstream parsing must tolerate missing characters.

## Local Workflows
- Install deps with `python -m pip install -r requirements.txt`; optional extras (e.g. `zstandard`) are commented when needed.
- Run the crawler via `python main.py --config config.yaml --seeds seeds.txt`; workspace paths resolve relative to `config.workspace` (default `./workspace`).
- Logs stream to stdout and `workspace/logs/crawler.log`; inspect `workspace/state/*.jsonl` to debug queue/robots cache state when resuming runs.
- The crawler is single-threaded; if you introduce concurrency, ensure `CrawlFrontier` persistence and rate limiting remain atomic (see `_rate_lock`).
- Tests are not present; when adding behavior, create focused async smoke scripts instead of attempting to reuse the removed legacy test harness.
