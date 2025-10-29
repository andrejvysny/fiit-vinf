# Extractor Module Overview

This document describes the current Python extraction tooling found in
`extract.py` and supporting utilities under `crawler/extractor.py`. The
pipeline converts downloaded GitHub HTML into multiple downstream artefacts:

- Plain-text dumps of the full page content (`text_*.html.txt`).
- Cleaned "main content" views with common chrome stripped
  (`main_*.html.txt`).
- Structured JSON metadata focused on repository signals
  (`structured_*.html.json`).
- Regex-derived entities (written to a TSV accumulator).

The extractor intentionally leaves crawling logic untouched; crawler outputs
the raw HTML, and `extract.py` owns all transformation.


## Key Components

- **`crawler.extractor.HtmlTextExtractor`** – Regex-driven HTML → text
  normaliser. It removes scripts/styles, converts structural tags to
  newlines, collapses whitespace, and returns both page title and cleaned
  text. This class is reused by higher-level helpers for consistent text
  rendering.

- **`GithubMainContentExtractor` (in `extract.py`)** – Wraps the
  `HtmlTextExtractor` to produce a boilerplate-free view. It parses the HTML
  with BeautifulSoup, prunes navigation-like elements (by tag name, ARIA role,
  class/id heuristics, etc.), then converts the reduced DOM back to text. A
  denylist removes leftover standalone nav words such as “sign in” or
  “marketplace”. The extractor tolerates malformed markup by guarding every
  attribute access.

- **`GithubRepoMetadataExtractor` (in `extract.py`)** – Collects repository
  signals from either DOM elements or embedded JSON payloads. It emits a
  `RepoMetadata` dataclass containing human-formatted counters and parsed
  integer equivalents, “About” copy, and README text (if it can be located).
  README detection first looks for Markdown articles in the DOM, then falls
  back to GitHub’s `application/json` payloads.

- **`crawler.extractor.RegexEntityExtractor`** – Runs configured regexes over
  the raw HTML to capture URLs, license mentions, and language hints. Results
  are appended to the shared TSV file with offsets.


## CLI Workflow (`extract.py`)

1. **Argument parsing** – Supports input/output roots, optional file lists,
   per-file overrides, `--limit`, `--force`, and verbosity toggles. Additional
   flags control destinations for main content and structured outputs.

2. **HTML selection** – `resolve_html_paths` aggregates paths from
   `--files-list`, explicit `--file` values, or a recursive scan beneath the
   input root. Non-`.html` entries are ignored, and `--limit` truncates the
   final list.

3. **Directory prep** – `ensure_parent` ensures text, main, and structured
   directories exist (created lazily). The entities TSV parent is also
   ensured prior to writes.

4. **Extractor initialisation** – Instantiates one `HtmlTextExtractor` (shared
   across helpers), the GitHub-specific main and metadata extractors, and the
   regex entity extractor. The TSV header is created on first write.

5. **Per-file processing** – For each HTML document:
   - Derives the document ID (`html_path.stem`) and stores the original HTML
     file name. Output files are named `text_{original}.html.txt`,
     `main_{original}.html.txt`, and `structured_{original}.html.json` to make
     provenance explicit.
   - Skips processing when all three outputs already exist and `--force` is
     absent; otherwise the HTML is loaded as UTF-8 (with errors ignored).
   - Runs the shared `HtmlTextExtractor` to produce the canonical text body.
   - Runs `GithubMainContentExtractor` to filter boilerplate and writes the
     cleaned view.
   - Runs `GithubRepoMetadataExtractor` and serialises the resulting dataclass
     via `json.dumps(..., indent=2, ensure_ascii=False)`.
   - Applies the regex entity extractor and appends rows to the TSV.
   - Logs progress every 25 files.

6. **Completion logging** – Reports processed/skipped counts and the realised
   output directories.


## Main Content Filtering Details

The main-content helper uses a conservative prune-and-keep strategy:

- **Tag-based removal** – Entire `<nav>`, `<header>`, `<footer>`, `<aside>`,
  `<form>`, and `<dialog>` sections are decomposed.

- **ARIA role filtering** – Elements with roles such as `banner`,
  `navigation`, `search`, `complementary`, or `contentinfo` are removed.

- **Keyword heuristics** – Classes/IDs containing strings like `header`,
  `UnderlineNav`, `AppHeader`, `sidebar`, `toolbar`, `js-repo-nav`, etc., are
  dropped. Data attributes (e.g. `data-test-selector`) are checked against the
  same dictionary.

- **Keep rules** – Markdown containers (`article[itemprop="text"]`, elements
  whose class includes `markdown` or `entry-content`) are preserved even if
  they match a broader heuristic.

- **Post-text cleanup** – After converting back to text, lines matching a
  denylist of chromey words are removed to avoid stray navigation labels.

All attribute access is guarded (via `hasattr` checks and cached `attrs`
dicts) so malformed nodes do not raise exceptions.


## Repository Metadata Extraction

- **Stars and forks** – Each metric is retrieved using multiple CSS selectors
  (`#repo-stars-counter-star`, `a[href$="/stargazers"] strong`, etc.). Raw
  strings are converted to integers via `_parse_human_count`, which handles
  `k`, `m`, and `b` suffixes and commas.

- **About section** – Searches for the first heading whose text is “About”
  (case-insensitive) and concatenates subsequent siblings until another
  heading appears. Repeated lines are deduplicated while preserving order.

- **README** – Prefers an inline Markdown article, otherwise walks any JSON
  blobs embedded in `<script type="application/json">` elements, looking for
  `richText` or `readme` payloads. Extracted HTML is run back through the
  shared text extractor for formatting consistency.

- **Resilience** – Any BeautifulSoup parsing errors are caught. Missing data
  simply yields `None` fields in the final JSON. A repository record with no
  discoverable metadata still serialises cleanly.


## Entity Extraction

`RegexEntityExtractor` iterates over compiled regex patterns that capture
licenses, import statements, `require()` calls, `#include`s, and URLs (HTML
anchors and Markdown links). Matches are written to the TSV with their byte
offsets relative to the original HTML string. A header row is added once per
run.


## Output Artefacts & Naming

- `text_<original>.html.txt` – Lossless plain text preserving full page and
  title.
- `main_<original>.html.txt` – Boilerplate-reduced text designed for downstream
  NLP ingestion.
- `structured_<original>.html.json` – JSON object with keys `title`, `stars`,
  `forks`, `about`, and `readme`. Counter subobjects include both formatted
  strings and integer estimates.
- `entities.tsv` – Tab-separated sheet appended across runs.

All text files are UTF-8 encoded. Paths are created on demand, and output
directories can be overridden via CLI flags.


## Testing

- `tests/test_html_text_extractor.py` – Regression coverage for the core
  HTML-to-text conversion.
- `tests/test_github_extractors.py` – Verifies boilerplate removal and metadata
  extraction (including fixture-based README parsing).
- `tests/test_extract_cli.py` – Exercises the CLI end-to-end, confirming the
  generation of all three artefact types and TSV output.


## Limitations & Future Considerations

- Navigation heuristics rely on keyword lists; niche layouts may slip through
  or remove legitimate content. Monitoring flagged keywords can guide future
  refinements.
- Metadata extraction targets GitHub’s current DOM/JSON structure. Changes to
  GitHub markup may require updates to selector lists.
- README extraction focuses on the main repository page; other surfaces (e.g.
  gists, issues) may need bespoke handling.
- Entity extraction is regex-only and language agnostic; richer models may be
  warranted for more nuanced signals.

Despite these caveats, the extractor is designed to fail gracefully—missing
data results in empty fields rather than raised errors—keeping the pipeline
robust across heterogeneous GitHub pages.

