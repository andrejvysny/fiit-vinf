# Project Overview

This repository contains a GitHub-focused crawler plus regex-based HTML text/entity extraction utilities that feed downstream indexing and search components. The current focus is stabilising data extraction for the 2b milestone (plain-text dumps, metadata TSVs, regex entity captures, and validation tests).

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

All commands below assume the virtual environment is active.

## Crawling GitHub

The crawler reads unified settings from `config.yaml` and seed URLs from `seeds.txt`.

```bash
python3 main.py --config config.yaml --seeds seeds.txt
```

Outputs (HTML blobs, metadata, logs) land under the configured `workspace/` tree. Adjust rate-limits, depth, and storage paths via `config.yaml`.

## Extraction CLI

`extract.py` provides a standalone runtime for transforming crawled HTML into plain text and entity annotations. By default it scans `workspace/store/html`, writes UTF‑8 text files, and appends entity matches into a TSV file.

```bash
# Basic run – process all HTML under the default workspace
python3 extract.py

# Limit to the first 100 files and store outputs under data/
python3 extract.py \
  --input-root workspace/store/html \
  --output-text data/text \
  --entities-file data/extract/entities.tsv \
  --limit 100

# Process a curated list of HTML paths (one per line, relative to --input-root)
python3 extract.py --files-list lists/interesting_pages.txt

# Force re-processing of existing text outputs
python3 extract.py --limit 10 --force

# Dry-run to inspect the selection without writing anything
python3 extract.py --limit 5 --dry-run
```

Each run logs progress (skipped vs processed) and reports the final text and TSV destinations.

## Running Unit Tests

Unit tests live in `tests/`. To exercise the extraction helpers and service utilities:

```bash
python3 -m unittest tests.test_html_text_extractor tests.test_link_extractor tests.test_crawler_service
# or discover everything under tests/
python3 -m unittest discover tests
```

All tests should pass with `OK`. Extend these tests as extraction rules evolve (e.g., additional boilerplate patterns or entity types).

## Next Steps

- Hook the extraction outputs into the indexing pipeline (`data/meta/docs.tsv`, `index/*`) for end-to-end search experiments.
- Expand regex entity coverage (imports, references, additional license variants) and add negative test cases.
- Document full pipeline (crawl → extract → index → search) on the project wiki per milestone requirements.
