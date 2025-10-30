## Extractor — Quick usage

This project includes an `extractor` module that extracts plain text, preprocessed text and structured entities from HTML pages (primarily GitHub pages). This short guide explains how to run the extractor and what outputs to expect.

## Entry points

- Run as a module (recommended):

  python -m extractor

- Or call the main function directly from Python code:

  from extractor.entity_main import main_entity_extraction
  main_entity_extraction()

## Default behaviour and paths

By default the extractor expects HTML files under `workspace/store/html` and writes outputs under `workspace/store`:

- Input HTML root: `workspace/store/html`
- Raw text output: `workspace/store/text` (mirrors input structure, `.txt` files)
- Preprocessed text: `workspace/store/text-preprocessed` (boilerplate removed)
- README extracts: `workspace/store/readme`
- Entities TSV: `workspace/store/entities/entities.tsv`

The extractor discovers HTML files recursively (glob `**/*.html`) and uses the HTML file stem as `doc_id` (filename without extension).

## Command-line flags

Common flags (see `--help` for full list):

- `--in` / `--input` : input HTML root (default: `workspace/store/html`)
- `--text-out` : directory for raw text (default: `workspace/store/text`)
- `--preproc-out` : directory for preprocessed text (default: `workspace/store/text-preprocessed`)
- `--readme-out` : directory for README files (default: `workspace/store/readme`)
- `--entities-out` : output TSV file for entities (default: `workspace/store/entities/entities.tsv`)
- `--limit N` : process at most N files
- `--sample N` : alias for `--limit` (useful for quick tests)
- `--force` : overwrite existing outputs
- `--dry-run` : list discovered files without processing
- `--no-text` / `--no-preproc` / `--no-entities` / `--no-readme` : disable specific outputs
- `--verbose` : enable debug logging

Examples:

- Process everything using defaults:

  python -m extractor

- Process 10 sample files and write only entities (no text files):

  python -m extractor --sample 10 --no-text

- Dry run to see which files would be processed:

  python -m extractor --dry-run

## Outputs and formats

- Raw and preprocessed text are saved as UTF-8 `.txt` files mirroring the input directory structure.

- Entities are written as a TSV file with header:

  doc_id\ttype\tvalue\toffsets_json

  - `doc_id`: file stem of the input HTML
  - `type`: entity type (see list below)
  - `value`: extracted value (string, or JSON string for structured values)
  - `offsets_json`: JSON array with `start`/`end` positions and `source` (`html` or `text`)

The extractor uses a `TSVWriter` that ensures the header is written once and escapes tabs/newlines in values by replacing them with spaces.

## What the extractor does (overview)

1. Reads an HTML file using `extractor.io_utils.read_text_file()`.
2. Produces two text variants using `extractor.html_clean.html_to_text()`:
   - raw text (tags removed, `strip_boilerplate=False`)
   - preprocessed text (boilerplate removed, `strip_boilerplate=True`)
3. Extracts README content via `entity_extractors.extract_readme()` and writes README text and README entities.
4. Extracts many entity types via `entity_extractors.extract_all_entities()` and writes them to the TSV.

The HTML-to-text routine removes GitHub navigation, headers, footers and other chrome (regex-driven, no DOM parser), normalizes whitespace, and filters minified code blobs.

## Entity types

The extractor extracts (but is not limited to) the following entity types:

- `STAR_COUNT` — repository star count (raw string like `34,222` or `34.2k`)
- `FORK_COUNT` — repository fork count
- `LANG_STATS` — JSON mapping of language name → percentage
- `README_SECTION` — extracted README text (returned as an entity row as well)
- `LICENSE` — SPDX or license link mentions
- `TOPIC` — repository topics/tags
- Import-related entities (Python/JS/C/Go/...) — import statements discovered in code blocks
- URLs — HTTP/HTTPS links or markdown/HTML links
- Issue references — `#123`, `owner/repo#123`, or issue/PR URLs
- Versions — semantic versions (semver-like)
- Emails — found email addresses
- Code fence language hints — language indicators from fenced code blocks

## Notes and troubleshooting

- The extractor intentionally uses regex-based HTML cleaning (no external DOM parser) for speed and portability; this is tuned for GitHub-like pages and may not perfectly remove all boilerplate for other sites.
- `doc_id` is derived from the HTML filename stem. If you change filenames, extracted entities will use the new `doc_id`.
- The preprocessor truncates very large README extracts to ~200k characters to avoid extremely large outputs.
- If you see missing entities, try running with `--force` or `--no-preproc` to debug raw text extraction.

## Example quick workflow

1. Create a small sample run:

   python -m extractor --sample 5 --verbose

2. Inspect `workspace/store/entities/entities.tsv` and `workspace/store/text-preprocessed/` for results.

3. When happy, run without `--sample`.

## Where to look in code

- Main orchestration: `extractor/entity_main.py`
- Regexes and entity patterns: `extractor/regexes.py`
- HTML cleaning & conversion: `extractor/html_clean.py`
- Entity extraction helpers: `extractor/entity_extractors.py`
- I/O helpers and TSV writer: `extractor/io_utils.py`

---

If you want a longer developer guide or example scripts (batching, parallel runs, or tests), I can add a `docs/extractor/usage.md` with more details and a small smoke-test script.
