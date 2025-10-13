## Quick orientation for code-writing agents

This repository is a small GitHub-focused crawler + scraper project. There are
two coexisting architectures in the repo:

- "separated" mode: `crawler/` produces discovery JSONL files in
  `workspace/spool/discoveries/` and `scraper/` tails that spool and stores
  HTML under `workspace/store/html/` (see `scraper/io/spool.py`).
- "unified" mode: a single process fetches, stores and extracts in one step
  (see `crawler/config_unified.py`, `crawler/run_unified.py`, `main.py`).

Read these files first for an overview and examples of patterns used here:

- `README.md` (high-level workflows and run examples)
- `main.py` (entry point for unified crawler)
- `run` (CLI wrapper: `./run configure`, `./run run --config config.yaml`)
- `crawler/run_unified.py` and `crawler/config_unified.py` (unified fetch+store)
- `scraper/run.py` and `scraper/io/spool.py` (separated scraper behaviour)

Key repo conventions and patterns (important to follow):

- File-based state: frontier, dedup, robots cache and metadata are stored as
  files under `workspace/state/` and `workspace/metadata/` (not hidden DBs).
- Content-addressed HTML store: files under `workspace/store/html/aa/bb/<sha>.html`
  (optionally `.html.zst`). Use `crawler/net/unified_fetcher.py` and
  `crawler/io/metadata_writer.py` as examples of how storage paths and metadata
  are produced and recorded.
- Discovery spool = JSONL lines of discovery objects. Bookmark is
  `workspace/state/spool_bookmark.json` (used by scraper to resume).
- Config-driven: behavior (scope, rate limits, caps, store paths) is defined
  in YAML files. See `crawler/config.py`, `crawler/config_unified.py`.
- Policy-first approach: URLs are checked against the `UrlPolicy` and robots
  cache before being enqueued (see `crawler/policy.py` and
  `crawler/robots_cache_file.py`). The config's `scope.allow_patterns` and
  `deny_patterns` are authoritative — don't bypass them.
- The user-agent string in config must include contact information and is
  validated by config loaders (see `CrawlerConfig` checks).

Common developer workflows (zsh/macOS):

1) Setup (venv + deps):

   source venv/bin/activate
   pip install -r requirements.txt

2) Create workspace scaffolding:

   ./run configure

3) Run unified crawler (recommended for local dev):

   ./run run --config config.yaml

   or directly:
   python main.py --config config.yaml --seeds seeds.txt

4) Run separated mode (crawler -> scraper):

   # Start producer (example from README)
   python -m crawler run --config config.yaml --period 2.0 --seeds seeds.txt

   # Start scraper (in another shell / tmux)
   ./vi-scrape run --config scraper_config.yaml

Debugging & important checks (follow these before editing network/fetch code):

- Check `workspace/logs/` for run-specific CSVs and scraper metrics.
- If scraper finds nothing to read, inspect `workspace/state/spool_bookmark.json`
  and `workspace/spool/discoveries/` files for naming/offset mismatches.
- When changing fetch or storage logic, preserve the content-addressed layout
  and update `crawler/io/metadata_writer.py` so CSV/JSONL metadata stays
  compatible with downstream tooling.

Integration points and external deps to be aware of:

- HTTP client: `httpx` is used for fetching (supports HTTP/2). See
  `crawler/net/unified_fetcher.py`.
- Parsers: `beautifulsoup4` / `selectolax` are used for link extraction
  (`crawler/parse/extractor.py`).
- Optional: LMDB and Bloom filter are referenced in older code paths—unified
  mode prefers simple file-based stores. Check `crawler/config.py` for both
  variants.

When proposing changes:

- Keep behavior controlled by config (add new options to `config*.py` and
  load them from YAML). Don't hardcode paths or timeouts.
- Add a small, focused unit or smoke test where possible (project uses
  pytest in dev-deps but may not have tests yet). Validate by running the
  CLI `status` helpers: `./vi-scrape status --config <cfg>` or
  `python -m crawler status --config <cfg>`.

If anything below is unclear or you need more examples from specific modules
(`frontier/*`, `io/*`, `net/*`, `parse/*`), tell me which area and I'll add
short, concrete snippets showing the exact inputs/outputs expected.
