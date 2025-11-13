# Repository Guidelines

## Project Structure & Module Organization
- `crawler/`, `extractor/`, and `indexer/` expose CLI entry points; each stage reads and writes shared `workspace/` artefacts.
- `workspace/` holds HTML snapshots, metadata, frontier state, and logs—keep it out of commits and treat it as runtime scratch space.
- `docs/` and `reports/` store generated summaries, `tools/` carries helper scripts, and `config.yml` centralizes defaults (use `--config` to override or `--help` for flags).
- `tests/` mirrors the main modules (`test_regexes.py`, `test_link_extractor.py`); add new cases beside the code they validate to keep context close.

## Build, Test, and Development Commands
- `python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt` – bootstrap the isolated interpreter that every command assumes.
- `python -m crawler --config config.yml` – crawl GitHub, write HTML to `workspace/store/html`, log metadata, and persist frontier state.
- `bin/spark_extract --config config.yml [flags]` – run the Dockerized PySpark extractor (starts `spark/docker-compose.yml`, submits `spark/jobs/html_extractor.py`, and mirrors `workspace/store/text` + `workspace/store/entities/entities.tsv`); pass `--local` to fall back to the pure-Python path.
- `python -m extractor --config config.yml` – legacy single-process extractor (kept for local smoke tests and as the `--local` fallback).
- `python3 -m indexer.build --config config.yml` – build JSONL documents, postings, and `manifest.json` from extractor outputs.
- `python -m unittest discover tests` – run the regression suite that covers regex and extractor helpers.

## Coding Style & Naming Conventions
- Follow Python/PEP 8: 4-space indentation, snake_case for variables/functions, PascalCase for classes, and use single quotes unless clarity demands double quotes.
- Group imports (stdlib, third-party, local), avoid unused symbols, and rely on `flake8` or a formatter if you run one locally.
- Tests live in `tests/`, follow the `test_<module>.py` pattern, and use `unittest.TestCase` with `test_*` methods so the discovery command catches them.

## Testing Guidelines
- Tests use Python’s built-in `unittest`; add regression cases under `tests/` close to the module being checked for easier navigation.
- Keep HTML/text fixtures deterministic (see `tests/test_regexes.py`) so regex coverage stays reproducible across runs.
- Run `python -m unittest discover tests` after modifying extraction, crawling, or indexing logic before opening a PR.

## Commit & Pull Request Guidelines
- Use concise, present-tense commits (`Improve crawler logging`, `Fix extractor entity offset`); capitalize the first word and describe the change’s intent.
- Pull requests should summarize the change, list verification steps, and link related issues if applicable; note when workspace artefacts or config defaults changed.
- Include the commands you executed (tests, builds) in the PR description so reviewers can duplicate your verification steps.
