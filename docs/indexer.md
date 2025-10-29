# Indexer usage guide

This document explains how to build and query the lightweight file-backed
inverted index provided by the `indexer` package in this repository.

## Quick summary

- Input: a directory with `.txt` files (default: `workspace/data/text`).
- Output: an index directory containing:
  - `docs.jsonl` — document table (one JSON object per line).
  - `postings.jsonl` — term postings and stored IDF values.
  - `manifest.json` — metadata (total_docs, total_terms, idf_method, idf_methods).
- Main CLIs:
  - Build: `python -m indexer.build`
  - Query: `python -m indexer.query`
  - Compare IDF methods: `python -m indexer.compare`

## Prerequisites

- Python 3.8+.
- Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

- Install dependencies:

```bash
python -m pip install -r requirements.txt
```

- Optional: to enable token counts during build (tiktoken):

```bash
python -m pip install tiktoken
```

If `tiktoken` is not installed and you request `--use-tokens`, the build will
raise a clear error.

## Building an index (CLI)

Build an index from text files. Default behavior (reads
`workspace/data/text`, writes `workspace/index/default`):

```bash
python -m indexer.build
```

Examples:

- Dry run (inspect dataset, do not write files):

```bash
python -m indexer.build --dry-run
```

- Build using RSJ idf in the manifest:

```bash
python -m indexer.build --idf-method rsj
```

- Limit number of documents (handy for tests):

```bash
python -m indexer.build --limit 100
```

- Include tiktoken-based token counts in `docs.jsonl` (requires
  `tiktoken`):

```bash
python -m indexer.build --use-tokens --token-model cl100k_base
```

What `build` does:

- Loads `.txt` files (recursively) under the input dir in deterministic,
  sorted order.
- Tokenizes each document using `indexer.tokenize`.
- Assigns `doc_id` by order (0-based).
- Builds postings: term -> {doc_id: term_frequency}.
- Computes IDF tables for supported methods (`log`, `rsj`).
- Writes `docs.jsonl`, `postings.jsonl`, and `manifest.json` atomically into
  output dir.

## Files produced (formats)

- `docs.jsonl` — one JSON object per line:
  - Keys: `doc_id` (int), `path` (string), `title` (string), `length` (int),
    optional `token_count`.
- `postings.jsonl` — one JSON object per line (one per term):
  - Keys: `term` (string), `df` (document frequency), `idf` (mapping
    method->float), `postings` (list of {"doc_id":int,"tf":int}).
- `manifest.json` — JSON object with:
  - `total_docs`, `total_terms`, `idf_method` (default used), and `idf_methods`.

Writes are atomic (temporary file + rename) to avoid partial files.

## Querying the index (CLI)

Simple interactive/one-shot query:

```bash
python -m indexer.query --query "github crawler"
```

Available flags:
- `--index` — path to index (default `workspace/index/default`)
- `--top` — number of results to return
- `--idf-method` — `manifest` (use stored idf), or `log`/`rsj` (compute those on
  the fly)
- `--show-path` — display full file path for results

Behavior:
- Uses the same tokenizer as build to parse queries.
- Scoring uses log-scaled tf and idf: tf-weight = 1 + log(tf), query-weight =
  1 + log(q_tf), score is sum(tf_weight * idf * query_weight) across matched
  terms.
- Deterministic tiebreaker: lower `doc_id` wins.

## Comparing IDF methods (CLI)

Generate a Markdown report that compares rankings for the `log` vs `rsj` IDF
methods:

```bash
python -m indexer.compare
```

Key options:
- `--index` — index directory
- `--query` (repeatable) or `--queries-file` — queries to compare
- `--top` — number of top documents to compare
- `--output` — Markdown output path (default `docs/generated/index_comparison.md`)

The script runs both methods and writes a side-by-side table with overlap and
Jaccard similarity for top-K results.

## Programmatic API

You can also use the indexer from Python.

Build programmatically (thin wrapper calls the CLI logic):

```py
from pathlib import Path
from indexer import build_index

summary = build_index(
    Path("workspace/data/text"),
    Path("workspace/index/default"),
    idf_method="log",
    limit=None,
    dry_run=False,
    use_tokens=False,
)
print(summary)  # {"documents": N, "terms": M}
```

Load and search an index:

```py
from pathlib import Path
from indexer.search import InvertedIndex

idx = InvertedIndex(Path("workspace/index/default"))
results = idx.search("async http client", top_k=5, idf_method="log", use_stored_idf=False)

for res in results:
    print(res.doc_id, res.score, res.title)
```

Important notes:
- `InvertedIndex` loads manifest, `docs.jsonl`, and `postings.jsonl` into
  memory; suitable for small/medium corpora.
- You can choose to use stored idf (from postings) or compute idf on-the-fly
  via arguments to `search`.

## Tokenization rules

- Tokenization is implemented in `indexer/tokenize.py`.
- It matches ASCII alphanumeric sequences: regex `[A-Za-z0-9]+`.
- Normalizes to lowercase.
- Drops tokens shorter than 2 characters.

Implication: non-ASCII letters (accented characters, non-Latin scripts) are
not matched by default. If you need multilingual tokenization, consider
updating the tokenizer.

## IDF methods (what they mean)

- `log` — smoothed log idf: `idf = log((N+1)/(df+1)) + 1`. Produces positive
  weights.
- `rsj` — RSJ/Robertson-Sparck Jones variant: `log((N - df + 0.5)/(df + 0.5))`
  with small clamps.
- The build step computes and stores idf for both methods; the manifest
  records a default `idf_method`.

## Determinism and recommendations

- Document ordering is deterministic: files discovered via sorted recursion,
  so repeated builds over the same dataset yield the same `doc_id`
  assignments.
- Postings and term ordering are written sorted to help diffing/reproducibility.
- For large datasets, current implementation loads all postings into memory;
  consider splitting or streaming if you need larger-scale indexing/search.

## Example quick workflow

```bash
# activate venv (zsh)
source .venv/bin/activate

# build index (dry-run)
python -m indexer.build --dry-run

# build real index
python -m indexer.build --input workspace/data/text --output workspace/index/default

# query the index
python -m indexer.query --query "repository metadata" --top 5 --show-path

# compare idf methods for a set of queries
python -m indexer.compare --queries-file queries.txt --top 10 --output docs/generated/index_comparison.md
```

## Limitations & suggestions

- Tokenizer only matches ASCII alphanumerics; it ignores accented and
  non-Latin scripts. If you need multilingual indexing, replace or extend
  `indexer.tokenize`.
- Memory: `InvertedIndex` fully materializes postings in memory; not suitable
  for very large corpora without modification.
- No tests included for scoring or tokenizer — consider adding unit tests for
  `tokenize`, `compute_idf`, and `InvertedIndex.search`.

---

If you want, I can also add example index outputs, small unit tests (pytest),
or make a tokenizer upgrade to support Unicode word characters. Tell me which
follow-up you'd like.
