# Repository Guidelines

## Project Structure & Module Organization
Core crawler services reside in `crawler/` (frontier management, fetch policy, metadata writers). Regex-driven text and entity extraction lives in `extractor/`, exposed through `python -m extractor`. The downstream indexing stack is grouped under `indexer/` (`build.py`, `ingest.py`, `search.py`). CLI helpers sit in `tools/` (for example `tools/crawl_stats.py`), while crawl configuration (including seeds) stays in `config.yaml` at the repository root. Test inputs and fixtures live in `tests/` and `tests_regex_samples/`. Runtime artifacts are written to `workspace/` with subdirectories that mirror the structure declared in `config.yaml`.

## Build, Test, and Development Commands
Activate the virtualenv before running anything:
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```
Start a crawl against the configured GitHub scopes:
```bash
python -m crawler --config config.yaml
```
Run extraction over stored HTML (defaults to `workspace/store/html`):
```bash
python -m extractor --limit 50 --entities-out workspace/store/entities/sample.tsv
```
Index previously extracted data for ad hoc queries:
```bash
python3 -m indexer.build --input workspace/store/text --output workspace/store/index/dev
```

## Coding Style & Naming Conventions
Follow standard Python 3 guidelines: 4-space indentation, snake_case for functions and variables, PascalCase for classes, and module-level constants in ALL_CAPS. Prefer explicit type hints (see `crawler/service.py`). Keep functions short and streaming-friendly to avoid buffering large HTML blobs in memory. Run `black .` (available via the optional dev dependency) before sending changes, and use `isort .` if you touch import-heavy modules.

## Testing Guidelines
Unit tests rely on `unittest`; target individual modules during development and run the full suite before opening a PR:
```bash
python3 -m unittest tests.test_crawler_service tests.test_link_extractor
python3 -m unittest discover tests
```
Fixture HTML samples in `tests_regex_samples/` should cover both positive and negative cases. Add regression tests whenever you tweak regex patterns, scope rules, or workspace paths.

## Commit & Pull Request Guidelines
Recent history leans toward Conventional Commits (`feat:`, `refactor:`, `remove`). Keep commits focused and reference issue IDs or Tasks.md items when relevant. In pull requests, include: 1) a concise summary of behavior changes, 2) command snippets showing how the change was verified, and 3) any crawl or extraction artifacts worth reviewing (path examples under `workspace/`). Attach screenshots only when UI/log formatting changes. Link related documentation updates in `docs/` or `USAGE.MD` so reviewers can trace the narrative.

## Configuration & Safety Notes
Keep personal tokens out of `config.yaml`; rely on environment variables if elevated access becomes necessary. Ensure `user_agents` always include a contact email per GitHub guidelines. For cautious crawl rollouts, start with a trimmed seeds list inside `config.yaml` and check workspace metadata before expanding. Monitor `extractor.log` and `workspace/logs/crawler.log` for anomalies after each run. Clean up `workspace/` directories instead of force-overwriting when testing destructive changes.
