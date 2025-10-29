# Project Requirements and Constraints

This document captures the detailed requirements (functional and non-functional), constraints, acceptance criteria, and recommended implementation notes for the GitHub crawler / extractor / indexer project. It is intended to be the single source-of-truth for what the system must provide for the 2b milestone and final submission.

## High-level objective

Build a robust, file-backed, resumable web crawler (target: GitHub) with a regex-based extraction pipeline and a simple indexing subsystem. Produce metadata and statistics for included wiki/report deliverables and provide reproducible CLI tools for crawling, extraction and statistics.

Deadlines and grading milestones (from course notes):
- Konzultácia 1 (project selection): 02.10.2025 — project description, seeds, sample Q/A pairs.
- Konzultácia 2a (design): 17.10.2025 — frameworks, architecture, headers/timeouts, initial crawl + some pages.
- Konzultácia 2b & first submission: 31.10.2025 — completed crawler, downloaded data + stats, extractor (regex) + wiki examples, own indexer with at least 2 IDF methods, tests for regex extractors (bonus: ≥20 pages), optional token statistics using `tiktoken`.

## Functional requirements

1. Crawler core
   - Input: YAML configuration file (canonical: `config.yaml`) and a seeds file (one URL per line, default `seeds.txt`).
   - Components: frontier, fetcher (unified fetch & store), crawl policy, metadata writer, link extractor, scheduler/loop.
   - Behavior:
     - Breadth-first frontier persisted on disk and resumable across restarts (`workspace/state/frontier.jsonl` and index dirs).
     - Deduplication of fetched URLs using `workspace/state/fetched_urls.txt` and index markers.
     - Respect allowlist/denylist policy and `robots.txt` (cache robots responses to disk).
     - Rate limiting, User-Agent rotation, exponential backoff and retry logic.
     - Sleep/randomized pauses configurable (per-request and batch pauses) to mimic polite crawling.
     - File-backed HTML storage: content addressed store under `workspace/store/html/aa/bb/<sha>.html`.
     - Unified metadata JSONL writer `workspace/metadata/crawl_metadata.jsonl` containing, at minimum, fields: `url`, `timestamp`, `depth`, `page_type`, `referrer`, `http_status`, `content_sha256`, `stored_path`, `content_bytes`, `content_type`, `fetch_latency_ms`, `retries`, plus optional extra metadata.

2. Crawl policy
   - Deny-by-default approach: only URLs that match at least one allow-pattern (configurable in `config.yaml`) and not matching deny-patterns are enqueued.
   - Allowed hosts and denied subdomains must be enforced (e.g., `github.com` only, deny `raw.githubusercontent.com`, etc.).
   - Robots.txt must be checked prior to enqueue or fetch. Cache entries to `workspace/state/robots_cache.jsonl` with TTL.
   - Classification of page types (topic, trending, repo_root, issues, pull, blob) for metadata.

3. Fetcher / storage
   - Use an async HTTP client supporting HTTP/2 where possible.
   - Honor configured timeouts (connect/read/total) and max retries with backoff.
   - Store raw HTML bytes in content-addressed layout using SHA-256; write atomically (tmp file → rename) and set file permissions as configured.
   - Return both metadata and decoded HTML string to caller for immediate extraction to avoid rereading stored file.

4. Extraction
   - Provide a CLI (`extract.py`) that scans `workspace/store/html` and writes:
     - Plain text dumps (`workspace/data/text`), cleaned main content (`workspace/data/main_content`), structured repo metadata JSONs (`workspace/data/structured`), and an entities TSV (`workspace/data/extract/entities.tsv`).
   - Implement regex-based HTML-to-text (`HtmlTextExtractor`) that removes scripts/styles/comments, preserves block structure, decodes entities, and normalizes whitespace.
   - Implement `RegexEntityExtractor` for entity types (LICENSE, LANG, IMPORT, URL, etc.) with start/end offsets and matched substrings.
   - Implement GitHub-specific cleaning extractors: `GithubMainContentExtractor` and `GithubRepoMetadataExtractor` to extract title, stars, forks, about, README.

5. Indexer and ranking
   - Provide an `indexer/` module or script that consumes the extracted text files and builds an inverted index and document table with TF values.
   - Implement at least two IDF methods and make them selectable at runtime:
     - IDF #1: standard log N/df (e.g., idf(term) = log(N / df)). Use smoothing (e.g., N / (df + 1)) if necessary to avoid division by zero.
     - IDF #2: RSJ/Robertson-Sparck-Jones variant (e.g., idf(term) = log((N - df + 0.5) / (df + 0.5))).
   - Provide a small ranking function that scores a query against a doc using term TF and chosen IDF and returns top-K results.
   - Index persistence may be JSONL or SQLite. CLI example: `python -m indexer.build --input workspace/data/text --output workspace/index/default --idf-method log`.

6. Statistics and reporting
   - Provide `tools/crawl_stats.py` to summarise `workspace/metadata/crawl_metadata.jsonl` and `workspace/state/service_stats.json` and produce optional Markdown/CSV outputs under `docs/generated/`.
   - Optional token-level statistics using `tiktoken` (import must be guarded so tool degrades gracefully if library is absent). Token stats should include: total files sampled, total tokens, avg tokens per file, avg chars per file.

7. Tests and validation
   - Unit tests covering:
     - HTML text extraction (happy path + a couple of real fixtures).
     - Link extraction (relative links, fragments, unquoted hrefs, duplicates).
     - Github metadata extractor on at least one real fixture.
   - Bonus/Grading requirement: unit tests for REGEX extractors across at least 20 real HTML pages (or an automated sampling harness using `tests/fixtures/html`).

8. Documentation and submission
   - Create wiki-style pages (use `docs/wiki_templates` as basis) for required milestones. Required sections for submission must include:
     - Project description (TLDR + motivation), seeds list, link to scraped sites and extracted data samples.
     - Frameworks and libraries used and rationale.
     - Architecture diagram and explanation of crawling pipeline.
     - Example metadata table (samples from `crawl_metadata.jsonl`).
     - Explanation and justification of chosen headers, timeouts, sleeps.
     - Links to real code for URL extraction and entity extraction (ideally per-file links in the repo).
     - Indexer design and methods for IDF (mathematical formulas and implementation notes), plus comparison of the methods on ≥3 queries.
     - A ZIP file containing your code for submission.

## Non-functional requirements and constraints

- Resumability: the crawler must persist frontier and fetched state to allow resume after interruption.
- File-backed only: no external DBs required; persist to `workspace/` using JSONL and filesystem indexes.
- Simplicity & reproducibility: CLI tools with minimal dependencies; optional extras (like `tiktoken`) must be optional and guarded.
- Safety: obey `robots.txt` and polite crawling rules (rate limiting, UA with contact info).
- Encoding: HTML is expected to be UTF-8; extraction must be robust to decoding errors (use errors='ignore' where necessary).
- Performance: conservative defaults in `config.yaml` (1 req/sec, small concurrency). Provide knobs to increase throughput but keep defaults safe.
- Tests should be runnable with a standard Python 3.11+ environment and `pip install -r requirements.txt`.

## Acceptance criteria (minimal)

To consider the milestone complete, the repository must contain and demonstrate the following:

1. The crawler code (frontier, fetcher, policy, metadata writer) runs and can be started via `python3 main.py --config config.yaml --seeds seeds.txt` and produces `workspace/store/html` and `workspace/metadata/crawl_metadata.jsonl` (or provide a short recorded run/log showing this).
2. The extraction CLI `extract.py` runs and writes `workspace/data/text`, `workspace/data/main_content`, `workspace/data/structured`, and `workspace/data/extract/entities.tsv` for processed HTML files.
3. The `tools/crawl_stats.py` script runs and produces Markdown and CSV output under `docs/generated/` when provided with `workspace` populated data. If `tiktoken` is installed, token stats are present.
4. An indexer exists (under `indexer/`) that builds a usable inverted index and implements at least two IDF formulas. Provide a small CLI to compare outputs of both IDF variants for at least three queries.
5. Unit tests pass locally (run `python -m pytest -q`), and extractor tests exist (bonus: tests over ≥20 pages).
6. Repository contains a short report or wiki pages derived from `docs/wiki_templates` and `docs/generated` that address the course submission checklist.

## Edge cases and error handling

- Non-HTML content: fetcher should detect non-HTML content-type and still store content, but mark it in metadata; extraction should skip non-HTML or attempt safe text conversion.
- Redirects: follow redirects but ensure canonicalization prevents fetching cycle or off-scope hosts.
- Rate-limiting and 429s: implement exponential backoff and honor `Retry-After` when present.
- Large files & memory: store bytes directly to disk; do not keep large decoded strings in memory unnecessarily; use streaming or chunked reads if needed.
- Robots parse errors: conservative behavior — deny or permit based on a clearly documented policy; current implementation errs on the side of denial when robots parsing fails.

## Implementation notes & suggestions

- Indexer storage: JSONL or a simple SQLite schema (documents table, postings table) are acceptable. SQLite reduces file-churn and supports fast lookups for DF/TF queries.
- Tokenization: for indexer use a small, deterministic whitespace + punctuation tokenizer (for reproducibility). Use `tiktoken` *only* for token counting/bonus, not for core indexing unless desired.
- IDF smoothing: when computing log(N/df), apply smoothing like log(1 + N/(1 + df)) or add epsilon in denominator to avoid division by zero.
- Tests: add a `tests/indexer_test.py` with a tiny corpus to verify both IDF variants produce expected rank ordering for simple queries.
- Reproducible runs: provide `preflight_environment()` usage notes (see `main.py`) and include a small `run_example.sh` or documented commands to reproduce a sample crawl/extract/index run.

## CLI examples and commands

Run crawler (preflight creates workspace layout):
```bash
python3 main.py --config config.yaml --seeds seeds.txt
```

Run extraction on stored HTML:
```bash
python3 extract.py --input-root workspace/store/html --output-text workspace/data/text --entities-file workspace/data/extract/entities.tsv --limit 100
```

Generate crawl statistics (and token stats if `tiktoken` is installed):
```bash
python3 tools/crawl_stats.py --workspace workspace --markdown-output docs/generated/crawl_stats.md --csv-output docs/generated/crawl_stats.csv --text-root workspace/data/text --token-model cl100k_base
```

Build index (example CLI to be implemented under `indexer/`):
```bash
python -m indexer.build --input workspace/data/text --output workspace/index/default --idf-method log
```

Run tests:
```bash
python -m pytest -q
```

## Deliverables checklist for submission

- Code: repository with working crawler, extractor, indexer (minimal), and `requirements.txt`.
- Data: `workspace/` containing produced HTML, metadata JSONL, and extracted text (or screenshots/logs showing a run if data is too large to include).
- Tests: unit tests included and passing.
- Report/wiki: pages under `docs/` or `docs/generated/` answering the course submission checklist items.
- ZIP: `submission.zip` containing the code and a README with commands to run.

## Open decisions for the team

- Persist index format: JSONL (easy) vs SQLite (safer for larger corpora). Recommendation: start with SQLite for postings and doc table.
- Tokenization rules for indexer: use simple word-level tokenization; optionally add normalization (lowercase, strip punctuation).
- Whether `tiktoken` should be added to `requirements.txt` (adds native dependency). Recommendation: keep optional; document how to install for bonus.

## Tech guidance — nepovolené, odporúčané a neodporúčané rámce a technológie

Pridávam tu konkrétne odporúčania a obmedzenia (pôvodný text v slovenčine nasledovne):

- Nepovolené: SQL databázy sú nepovolené; ak potrebujete niečo uložiť, použite CSV/TSV súbory (TSV preferované).
- NoSQL databázy sú povolené (ak je potrebné uložiť veľké dataset-y). Príklady povolených technológií: Cassandra, Hive, HBase — tieto sú dostupné v infraštruktúre a sú prípustné.
- Jazyky: Python a Java sú vysoko odporúčané.
- Distribuované spracovanie: odporúčam Spark; klasický Hadoop alebo Pig sú tiež prípustné pri vhodných úlohách.
- Indexovanie: odporúčané použitie Lucene (pre Java). Pre Python možno použiť PyLucene (napr. v Docker image) alebo Solr; povolené je aj postaviť vlastný index (viac práce).
- Vyhľadávanie a evaluácia: postačuje command-line rozhranie; evaluáciu vykonajte porovnaním výsledkov na niekoľkých dopytoch alebo, ak je k dispozícii, použitím nejakého zlatého štandardu.
- Pure Python zásady: povolené knižnice a nástroje: `re`, `requests`, `selenium`.
   - Nepovolené/odporúčané vynechať: NIE `pandas`, NIE `nltk` (NLTK sa odporúča ignorovať — primárne je pre angličtinu a nie je preferované pre tento kurz).

Poznámky:
- Ak zvolíte NoSQL alebo distribuované rámce, uveďte v dokumentácii dôvod pre ich použitie a ako ich nasadiť/reprodukovať (skripty, docker-compose, alebo krátky popis infraštruktúry).
- Ak použijete Solr/Elasticsearch/Lucene via Docker, zahrňte inštrukcie alebo docker-compose príklad v `docs/`.

## Appendix: mapping of repo components to requirements

- Crawler engine: `crawler/service.py`, `crawler/unified_fetcher.py`, `crawler/crawl_frontier.py`, `crawler/crawl_policy.py`, `crawler/metadata_writer.py`.
- HTML/text extraction: `crawler/extractor.py`, `extract.py` CLI, tests in `tests/`.
- Crawl stats & token counting: `tools/crawl_stats.py` (tiktoken guarded).
- Indexer: NOT IMPLEMENTED — implement under `indexer/` (see suggested CLI above).
- Wiki templates and docs: `docs/wiki_templates/` contain helpful scaffolds — fill them out for submission.

---

If you want, I can now: run the test suite and report results, or start a minimal `indexer/` implementation (TF + 2 IDF methods + tests). Which should I do next? If you want the `Requirements.md` adjusted (more formal, or converted to a checklist), tell me the preferred format.
