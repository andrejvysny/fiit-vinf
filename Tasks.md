# Implementation Tasks for missing features

This file lists actionable, developer-friendly tasks to implement the missing features in the repository and to satisfy the course assignment checklist. Each task is self-contained and includes: context, files to change or add, acceptance criteria, suggested implementation notes, and commands to run for verification.

If you want me to implement any of these tasks, tell me which task ID(s) to start and I will create the code, tests and run the verification steps.

## Repo context (short)
- Project root contains: `main.py`, `config.yaml`, `extract.py`, `crawler/`, `tools/crawl_stats.py`, `tests/`, `docs/`, `workspace/`.
- Implemented: crawler core (`crawler/service.py`, `crawler/unified_fetcher.py`, `crawler/crawl_frontier.py`, `crawler/crawl_policy.py`, `crawler/metadata_writer.py`), HTML/text extraction (`crawler/extractor.py`) and extraction CLI (`extract.py`).
- Missing: indexing subsystem (TF/IDF), IDF comparison harness, expanded extractor fixtures/tests (20 pages), final wiki/report pages, submission helper scripts.

## Prerequisites for implementers
- Python 3.10+ (project appears to target modern Python). Use virtualenv.
- Install project requirements:

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

- Optional: install `tiktoken` for token counting (bonus): `pip install tiktoken`

Run unit tests locally while developing:

```bash
python -m pytest -q
```

## Task list (detailed)

Task ID: T1 — Run baseline tests and capture results
- Purpose: Establish a baseline and record failing tests before changes.
- Actions:
  - Run `python -m pytest -q` and save output to `docs/generated/test_report.txt`.
  - If tests fail for unrelated reasons (missing deps), note them and fix small environment issues (e.g., install `bs4`, `httpx`) only if necessary.
- Files changed: none (create `docs/generated/test_report.txt`).
- Acceptance: `docs/generated/test_report.txt` exists and contains the test run summary.

Task ID: T2 — Create indexer package skeleton
- Purpose: Add `indexer/` package and minimal CLI to ingest text and store TF/DF.
- Files to add:
  - `indexer/__init__.py`
  - `indexer/build.py` — CLI: `python -m indexer.build --input workspace/data/text --output workspace/index/default` with `--dry-run` and `--idf-method` flags.
  - `indexer/tokenize.py` — deterministic tokenizer: `tokenize(text) -> List[str]`.
  - `indexer/ingest.py` — read files, compute TF per doc, write doc metadata.
  - `indexer/store.py` — write JSONL index files (`docs.jsonl`, `postings.jsonl`) under output directory.
  - `indexer/README.md` — usage & examples.
- Suggested implementation notes:
  - Keep tokenizer simple: lowercase, split on non-word characters, drop tokens with len < 2.
  - Use JSONL for ease of inspection: one doc per line.
- Acceptance criteria:
  - `python -m indexer.build --input workspace/data/text --output workspace/index/default --dry-run` prints selected files and exits without writing.
  - `python -m indexer.build --input <small-corpus> --output <out>` writes `docs.jsonl` and `postings.jsonl` in `<out>`.

Task ID: T3 — Implement TF and two IDF formulas
- Purpose: Compute TF per doc and implement selectable IDF formulas.
- Files to modify/add:
  - `indexer/ingest.py` (TF computation)
  - `indexer/store.py` (persist DF and postings)
  - `indexer/build.py` (add `--idf-method` arg and compute idf table)
- IDF formulas to implement (with safe smoothing):
  - `log` (default): idf = log(1 + N / (1 + df))
  - `rsj`: idf = log((N - df + 0.5) / (df + 0.5)) — clamp values to avoid negative/inf.
- Acceptance criteria:
  - Index build completes and produces `idf.json` or a header in `postings.jsonl` with df and idf per term.
  - Small unit tests (see T7) confirm numerical correctness for toy corpus.

Task ID: T4 — Add ranking CLI and comparison harness
- Purpose: Allow quick experiments to compare IDF methods on queries.
- Files to add:
  - `indexer/query.py` — load index, run query, print top-K.
  - `indexer/compare.py` — run several queries and write `docs/generated/index_comparison.md` with rankings for `log` and `rsj` methods.
- Behavior:
  - CLI: `python -m indexer.query --index workspace/index/default --query "some terms" --top 10 --idf-method log`.
- Acceptance:
  - Running `indexer/compare.py` with 3 queries produces `docs/generated/index_comparison.md` showing comparison tables.

Task ID: T5 — Integrate optional token counting (`tiktoken`)
- Purpose: Provide token counts for docs (bonus grading item).
- Files to modify:
  - `indexer/ingest.py` — optional `--use-tokens` flag to compute tokens per doc when `tiktoken` is installed.
  - `tools/crawl_stats.py` — already supports tiktoken; ensure docs mention installation.
- Notes:
  - Guard imports: try/except ImportError and produce a friendly message if missing.
- Acceptance:
  - When `tiktoken` is installed, `indexer.build --use-tokens` adds `token_count` to each doc entry in `docs.jsonl`.

Task ID: T6 — Expand extractor tests & fixtures to ~20 pages
- Purpose: Meet bonus test coverage for regex extractors.
- Actions:
  - Collect up to 20 HTML fixtures (small, sanitized) and put under `tests/fixtures/html/`.
  - Add test `tests/test_extract_fixtures.py` parameterized over fixtures; asserts extractor returns non-empty text and no HTML tags remain.
  - Avoid adding large binary assets; keep fixtures trimmed for repo size.
- Acceptance:
  - `pytest` runs the new test and it passes locally.

Task ID: T7 — Add indexer unit tests
- Purpose: Validate ingest, TF/DF, IDF formulas, and ranking on synthetic corpus.
- Files to add:
  - `tests/test_indexer.py` — create temporary files for a small corpus (3–5 docs), run `indexer.build` on them, assert computed DF/IDF values and ranking order for sample queries.
- Acceptance:
  - Tests run quickly and pass under `pytest`.

Task ID: T8 — Docs: generate wiki pages and final report
- Purpose: Produce the required wiki/report content and map assignment checklist to implemented features.
- Actions:
  - Use `docs/wiki_templates/konzultacia-*.md` as base and generate `docs/generated/konzultacia-1.md` and `docs/generated/konzultacia-2.md` filled with project-specific content.
  - Run `tools/crawl_stats.py` to produce `docs/generated/crawl_stats.md` and include index comparison results in `docs/generated/index_comparison.md`.
  - Add architecture diagram (ascii or small PNG) under `docs/generated/` and reference code files.
- Acceptance:
  - `docs/generated/final_report.md` exists and covers all submission checklist items.

Task ID: T9 — Add submission helper script
- Purpose: Create `scripts/make_submission.sh` to create `submission.zip` excluding `workspace/` and virtualenv.
- Script example steps:
  - `git ls-files > files.txt` then filter out workspace files OR use `zip -r submission.zip . -x workspace/* -x venv/* -x .git/*`.
  - Include `docs/generated/final_report.md` and tests.
- Acceptance: running `bash scripts/make_submission.sh` produces `submission.zip` under repo root.

Task ID: T10 — Update requirements & README
- Purpose: Document optional dependencies and indexer usage.
- Actions:
  - Add a commented entry for `tiktoken` in `requirements.txt` (e.g., `# tiktoken==0.x.x  # optional, for token stats`).
  - Update `README.md` with indexer usage examples and commands to run tests and generate docs.
- Acceptance: `README.md` contains indexer usage and `requirements.txt` mentions optional token lib.

Task ID: T11 — Full validation run
- Purpose: Verify end-to-end: crawl (or sample data), extract, index, stats, tests.
- Actions:
  - Run `extract.py` on a small set of HTML fixtures or a few stored HTML files.
  - Run `python -m indexer.build` to build index.
  - Run `python -m indexer.compare` and `python tools/crawl_stats.py` to produce generated docs.
  - Run `python -m pytest -q` and ensure all tests pass.
- Acceptance: all tasks complete and `docs/generated` contains the required artifacts.

Task ID: T12 — Final report & checklist
- Purpose: Create `docs/generated/final_report.md` describing what is implemented, where, and instructions to reproduce the results.
- Actions:
  - Summarize mapping of assignment checklist to repo files and generated outputs.
  - Include a short checklist and links to the generated docs and the `submission.zip` artifact.
- Acceptance: `docs/generated/final_report.md` is present and referenced in `scripts/make_submission.sh`.

## Verification commands (summary)
- Run tests: `python -m pytest -q`
- Build index (dry-run):
  - `python -m indexer.build --input workspace/data/text --output workspace/index/default --dry-run`
- Build index (real):
  - `python -m indexer.build --input workspace/data/text --output workspace/index/default --idf-method log`
- Query index:
  - `python -m indexer.query --index workspace/index/default --query "your query" --top 10 --idf-method rsj`
- Generate crawl stats:
  - `python3 tools/crawl_stats.py --workspace workspace --markdown-output docs/generated/crawl_stats.md --csv-output docs/generated/crawl_stats.csv --text-root workspace/data/text`

## Notes for coding agents
- Keep changes small and well-tested. Prefer adding new files under `indexer/` and `tests/` rather than modifying core extractor code unless necessary.
- When adding fixtures, minimize size. Prefer stripped HTML (no images) to keep repo lightweight.
- For index persistence choose JSONL first for simplicity; if you need better performance or ACID guarantees consider SQLite later (provide a compatibility layer in `indexer/store.py`).

---

If you'd like, I can start by executing T1 (run tests) or T2 (create indexer skeleton). Which should I start implementing now? 
