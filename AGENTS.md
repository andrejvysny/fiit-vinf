# Repository Guidelines

## Project Structure & Module Organization
- `bin/cli` is the unified entry point; core logic lives in `crawler/`, `extractor/`, `indexer/`, `lucene_indexer/`, and shared Spark jobs under `spark/jobs/` + helpers in `spark/lib/`.
- Runtime artifacts stay in `workspace/store/` (`html/`, `text/`, `spark/entities/`, `wiki/`, `join/`, `index/`, `lucene_index/`); keep the workspace out of commits.
- Tests sit in `tests/` (unittest); reference docs in `README.md`, `FULL_RUN_GUIDE.md`, and `docs/`.
- Configuration is centralized in `config.yml`; update paths, crawler scope, and memory defaults there before runs.

## Build, Test, and Development Commands
- Bootstrap: `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt`.
- Full pipeline: `bin/cli pipeline`; smoke run: `bin/cli pipeline --sample 100 --wiki-max-pages 1000`.
- Stage by stage: `bin/cli extract`, `bin/cli wiki`, `bin/cli join`, `bin/cli lucene-build`, `bin/cli lucene-search "python web"`.
- Docker Compose equivalents: `docker compose run --rm spark-extract|spark-wiki|spark-join|lucene-build|lucene-search`; adjust memory per stage, e.g., `SPARK_DRIVER_MEMORY=8g SPARK_EXECUTOR_MEMORY=4g bin/cli wiki`.

## Coding Style & Naming Conventions
- Python 3.9+, PEP 8, 4-space indents; snake_case for vars/functions, CapWords for classes; keep modules focused and small.
- Maintain streaming safety in Spark jobs (e.g., avoid `.cache()` in wiki extractor) and keep document IDs derived from HTML filename stems.
- Prefer pure utilities in `spark/lib/`; route batch logs to `logs/*.jsonl` instead of noisy stdout.
- CLI flags are kebab-case; mirror config keys to module names for clarity.

## Testing Guidelines
- Run all tests with `python -m unittest discover tests`; target files via `python -m unittest tests.test_regexes` or `tests.test_spark_extractor`.
- Add/adjust tests whenever touching regexes, parsing, or Spark transformations; keep fixtures deterministic and small for CI.
- For Spark-heavy changes, validate first with `--sample` or `--wiki-max-pages` to prove logic before large runs.

## Commit & Pull Request Guidelines
- Commit messages here use concise imperatives or prefixes (`feat:`, `fix:`); keep subjects ≤72 chars and focused on the change.
- PRs should describe scope, list commands run, note dataset/sample sizes, and attach relevant logs or search snippets.
- Do not commit generated data under `workspace/` or `logs/`; document any manual prerequisites (e.g., `wiki_dump/*.xml` location).

## Security & Configuration Tips
- Keep tokens, crawl seeds, and credentials out of version control; prefer env vars or untracked local `config.yml` overrides.
- Wikipedia dumps and crawl outputs are large—verify disk space and tune `SPARK_DRIVER_MEMORY`/`PARTITIONS` in the environment before starting long jobs.
