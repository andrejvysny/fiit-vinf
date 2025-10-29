# Implementation Plan

## Task 1 – Wiki Deliverable Templates
- **Objective**: Ensure the project wiki aligns with the required structure and contains the narrative artefacts for Konzultácia 1, Konzultácia 2a, and the final report. The plan is to scaffold markdown templates that can be copied to the wiki, enabling quick updates as technical work lands.
- **Scope**:
  - Draft `start` page stub outlining project TLDR, motivation, and navigation to all required subpages.
  - Prepare dedicated markdown templates for “1. Konzultacia”, “2. Konzultacia”, … “5. Konzultacia”, plus “1. Odovzdanie” and “2. Odovzdani” to mirror the mandated wiki tree.
  - Capture initial content for Konzultácia 1 (project overview, chosen data sources, wiki integrations, five Q&A pairs) and Konzultácia 2a (framework choices, architecture diagram reference, header/timeout plan).
  - Document instructions for synchronising repo markdown to school wiki, including the `[[user:meno.priezvisko:site_name]]` hint usage.
- **Prerequisites**: Consolidated requirements (already analysed), up-to-date knowledge of current architecture diagrams (existing in `docs/ARCHITECTURE.md`).
- **Deliverables**: Markdown files (e.g., under `docs/wiki_templates/`) containing structured sections with TODO markers for data that will be auto-filled later by other tasks.
- **Open Questions**: Confirm whether the wiki accepts HTML diagrams or only embedded links; verify naming conventions for the STU wiki pages.

## Task 2 – Crawl Statistics Toolkit
- **Objective**: Automate reporting of crawl coverage and resource usage to populate the final report and wiki tables.
- **Scope**:
  - Implement `tools/crawl_stats.py` that reads `workspace/state/service_stats.json` (for counters) and scans `workspace/metadata/crawl_metadata.jsonl` (for per-document insights such as HTTP status, size, page type).
  - Compute required statistics: total documents, total size (formatted/readable), breakdown by page type (repo, issue, pull), number of relevant documents (configurable filter), and failure counts.
  - Optional: integrate `tiktoken` to compute token counts of stored text dumps; guard the import so the script degrades gracefully if the package is absent (needed for bonus credit).
  - Output formats: CLI stdout summary, Markdown table emitter (saved under `docs/generated/crawl_stats.md`), and CSV for external analysis.
  - Provide CLI parameters to point at alternate workspaces or to limit sample sizes for quick runs.
- **Prerequisites**: Access to `workspace/` artefacts; no additional dependencies beyond optional `tiktoken`.
- **Deliverables**: Script within `tools/`, unit tests mocking small metadata sets, and documentation in README/wiki on how to regenerate stats.
- **Risks**: Large metadata files – ensure streaming processing to avoid high RAM usage.

## Task 3 – Extraction Showcase Pipeline
- **Objective**: Produce demonstrable extraction artefacts (text, main content, metadata, regex samples) ready for embedding into wiki/report.
- **Scope**:
  - Build `tools/extraction_samples.py` that selects five representative HTML files (random sample or targeted repository pages).
  - Run `extract.py` programmatically or via subprocess to generate outputs in a temporary directory.
  - Collect snippets (first N lines) from each artefact type and save to Markdown/JSON under `docs/generated/extraction_samples/`.
  - Include entity TSV excerpts highlighting regex matches and describe their meaning.
  - Provide CLI options to choose specific pages or regenerate all samples.
- **Prerequisites**: `extract.py` functioning; enough HTML files in workspace.
- **Deliverables**: Sample artefacts, summary Markdown for wiki inclusion, and optional screenshot-ready text blocks.
- **Open Questions**: Determine criteria for “representative” (e.g., mix of repo root, issue, pull request).

## Task 4 – Indexing Module Implementation
- **Objective**: Create an indexing subsystem that consumes extracted text and prepares data structures for search/ranking.
- **Scope**:
  - Introduce `indexer/` package with modules for corpus ingestion, inverted index construction, and persistence (likely JSONL or SQLite for simplicity).
  - Define data model: document records referencing text files, tokenizers, stopword handling.
  - Implement term frequency (TF) tracking per document and store aggregated metadata (document length, vocabulary size).
  - Provide CLI `python -m indexer.build --input workspace/data/text --output workspace/index/default` with options for incremental rebuilds.
  - Ensure the pipeline records metadata needed for final reports (e.g., number of times index rebuilt, doc counts).
- **Prerequisites**: Text outputs from extraction step; decision on tokenizer (std library split or `re`-based).
- **Deliverables**: `indexer` package code, configuration file (if needed), and README section describing usage.
- **Risks**: Index size/complexity – start with manageable format, leave note for scaling improvements.

## Task 5 – Dual IDF Strategies
- **Objective**: Implement at least two ranking strategies differing in their IDF computation to satisfy milestone requirement.
- **Scope**:
  - Extend `indexer` to calculate traditional logarithmic IDF (`idf_log = log((N + 1)/(df + 1)) + 1`) and probabilistic RSJ (`idf_rsj = log((N - df + 0.5)/(df + 0.5))`).
  - Persist both IDF values alongside term statistics for efficient query-time retrieval.
  - Provide configuration toggles to pick default method and allow future expansions (BM25, pivoted length).
  - Document formulas and rationale for the wiki.
- **Deliverables**: Implementation within indexer, unit tests ensuring formulas produce expected values on toy corpora, Markdown snippet explaining the math.

## Task 6 – Query Comparison CLI
- **Objective**: Compare search behaviour across the two IDF strategies on at least three queries and capture results for reporting.
- **Scope**:
  - Create `indexer/query.py` (or CLI entry) to accept `--method` flag (e.g., `log`, `rsj`), run ranking, and output top-k results with scores and doc metadata.
  - Implement script `tools/compare_idf_methods.py` that runs predefined queries against both methods, assembles comparison tables, and writes Markdown to `docs/generated/idf_comparison.md`.
  - Include summary metrics (e.g., overlap, distinct top results).
- **Deliverables**: CLI tools, generated Markdown for wiki/report, tests using miniature index fixtures verifying output format.
- **Prerequisites**: Completed Task 4 & 5.

## Task 7 – Expanded Automated Tests
- **Objective**: Strengthen test coverage for regex extraction and the new indexer to reduce regression risk.
- **Scope**:
  - Gather ≥20 HTML fixtures (existing ones plus additional crawl samples) and place under `tests/fixtures/html_extended/`.
  - Write tests that iterate over the fixtures to ensure regex extractor captures expected entities and avoids known false positives.
  - Add unit tests for index builder (correct TF/IDF values, persistence), query execution, and CLI integration (using temporary directories).
  - Consider property-based tests for tokenization and ranking tie-breakers.
- **Deliverables**: New fixtures, test modules (`tests/test_indexer_build.py`, `tests/test_idf_methods.py`, etc.), updates to test runner instructions.
- **Risks**: Test runtime – may need to limit fixture size; ensure deterministic samples.

## Task 8 – Dependency & Packaging Updates
- **Objective**: Finalise environment setup, packaging instructions, and deliverable artefacts.
- **Scope**:
  - Update `requirements.txt` with any new libraries (e.g., `tiktoken`, optional `numpy` if used for ranking).
  - Enhance README with framework justification (crawler stack, BeautifulSoup, httpx) and instructions for the new tools.
  - Script generation of the submission ZIP (e.g., `tools/make_submission.py`) bundling code, generated docs, and tests per requirement.
  - Ensure documentation points to real code snippets for URL extraction and entity extraction, fulfilling report checklist.
- **Deliverables**: Updated dependencies, README sections, submission script, and cross-links from docs to code.
- **Dependencies**: Completion of earlier tasks to reference accurate tooling in documentation.

---

### Timeline & Dependencies Overview
1. Tasks 1–3 focus on documentation scaffolding and data reporting – they can start immediately and run in parallel.
2. Tasks 4–6 depend on extraction outputs; Task 4 must finish before Tasks 5 and 6.
3. Task 7 spans the whole effort but should begin once indexer prototypes exist to avoid rework.
4. Task 8 wraps everything by packaging dependencies and submission assets; schedule last but start drafting README updates earlier.

Ensure each task records command invocations and generated artefacts so the wiki/report can reference reproducible steps.
