# Indexer Module

The `indexer` package builds a lightweight inverted index over the text files
produced by `extract.py`. The goal is to provide a reproducible, file-backed
search layer without external dependencies.

## CLI

```bash
python -m indexer.build \
  --input workspace/data/text \
  --output workspace/index/default \
  --idf-method log
```

Flags:

- `--input` – root directory with `.txt` documents (defaults to
  `workspace/data/text`).
- `--output` – directory where index artefacts will be written (defaults to
  `workspace/index/default`).
- `--idf-method` – weighting scheme (`log` or `rsj`).
- `--limit` – optional upper bound on processed documents (handy for tests).
- `--dry-run` – inspect dataset without writing files.

Outputs:

- `docs.jsonl` – document table (`doc_id`, `path`, `title`, token length).
- `postings.jsonl` – sorted vocabulary with DF/IDF and postings lists.
- `manifest.json` – summary metadata (document & term counts, IDF method).

The resulting files will be consumed by the upcoming query and comparison
tools as well as grading scripts. Refer to `python -m indexer.build --help`
for the latest options.
