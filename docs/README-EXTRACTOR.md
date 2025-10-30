# Entity Extractor - Documentation

A regex-based entity extractor for GitHub HTML pages. Extracts structured entities, repository metadata, and generates preprocessed text files. **Uses only Python stdlib** (no external dependencies).

## Features

- **Entity Extraction**: Extracts 13+ entity types from HTML and text
- **GitHub Metadata**: Stars, forks, language statistics
- **README Extraction**: Dedicated README text extraction
- **Preprocessed Text**: Clean text with boilerplate removal
- **TSV Output**: All entities in a single TSV file with offsets
- **Modular Architecture**: Small, testable, reusable components

## Installation

No installation required. Uses only Python standard library:
```python
import re, html, json, os, pathlib, argparse, logging, typing
```

## Quick Start

```bash
# Run entity extraction on all HTML files (using defaults)
python -m extractor

# Process first 10 files only (for testing)
python -m extractor --sample 10

# With verbose logging
python -m extractor --sample 10 --verbose

# Custom paths
python -m extractor \
  --in workspace/store/html \
  --entities-out workspace/store/entities/entities.tsv \
  --preproc-out workspace/store/text-preprocessed \
  --readme-out workspace/store/readme

# Run smoke test
python tests/smoke_test.py
```

## CLI Usage

The extractor has a unified entrypoint through `python -m extractor`:

```bash
python -m extractor [OPTIONS]
```

**Running without arguments uses default paths and processes all files:**
- Input: `workspace/store/html`
- Entities: `workspace/store/entities/entities.tsv`
- Preprocessed text: `workspace/store/text-preprocessed`
- README files: `workspace/store/readme`

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--in`, `--input` | `workspace/store/html` | Input directory with HTML files |
| `--entities-out` | `workspace/store/entities/entities.tsv` | Output TSV for entities |
| `--preproc-out` | `workspace/store/text-preprocessed` | Output directory for preprocessed text |
| `--readme-out` | `workspace/store/readme` | Output directory for README files |
| `--limit N` | (none) | Process at most N files |
| `--sample N` | (none) | Process N sample files (alias for --limit) |
| `--dry-run` | (false) | List files without processing |
| `--verbose` | (false) | Enable debug logging |

### Examples

```bash
# Full extraction with defaults
python -m extractor

# Test with 5 samples
python -m extractor --sample 5 --verbose

# Custom paths
python -m extractor \
  --in /path/to/html \
  --entities-out /path/to/output.tsv \
  --preproc-out /path/to/text \
  --readme-out /path/to/readme

# Dry run (list files only)
python -m extractor --dry-run

# Show help
python -m extractor --help
```

## Outputs

### 1. Entities TSV (`entities.tsv`)

Tab-separated file with schema:

```
doc_id    type    value    offsets_json
```

- **doc_id**: Document identifier (HTML filename without extension)
- **type**: Entity type (see below)
- **value**: Extracted value (raw string or JSON for complex types)
- **offsets_json**: JSON array of `{"start": int, "end": int, "source": "html"|"text"}` objects

#### Entity Types

| Type | Description | Value Format | Source |
|------|-------------|--------------|--------|
| `STAR_COUNT` | Repository star count | Integer string (e.g., "1200") | HTML |
| `FORK_COUNT` | Repository fork count | Integer string | HTML |
| `LANG_STATS` | Language statistics | JSON: `{"Python": 73.5, "C++": 21.2}` | HTML |
| `README_SECTION` | Main README text | Plain text (capped at 200k chars) | HTML |
| `LICENSE` | License identifier | SPDX ID (e.g., "MIT", "Apache-2.0") | HTML & Text |
| `TOPIC` | Repository topic/tag | String (e.g., "machine-learning") | HTML |
| `IMPORT` | Import statement | Module name | Text |
| `URL` | HTTP/HTTPS URL | Full URL string | HTML & Text |
| `ISSUE_REF` | Issue reference | `#123`, `owner/repo#123`, etc. | Text |
| `VERSION` | Semantic version | `1.2.3`, `1.0.0-alpha` (no leading 'v') | Text |
| `EMAIL` | Email address | Normalized email | Text |
| `LANG` | Code block language | Language hint (e.g., "python", "rust") | Text |

#### Example Rows

```tsv
doc_id    type          value                                        offsets_json
abc123    STAR_COUNT    1200                                         [{"start":10234,"end":10238,"source":"html"}]
abc123    FORK_COUNT    210                                          [{"start":10450,"end":10453,"source":"html"}]
abc123    LANG_STATS    {"Python":73.5,"C++":21.2,"Other":5.3}       [{"start":2030,"end":2300,"source":"html"}]
abc123    README_SECTION    Getting started...\nInstall...           [{"start":4500,"end":9800,"source":"html"}]
abc123    LICENSE       MIT                                          [{"start":1100,"end":1103,"source":"html"}]
abc123    TOPIC         machine-learning                             [{"start":3200,"end":3220,"source":"html"}]
abc123    IMPORT        numpy                                        [{"start":450,"end":465,"source":"text"}]
abc123    URL           https://github.com/owner/repo                [{"start":890,"end":920,"source":"html"}]
abc123    ISSUE_REF     #123                                         [{"start":1200,"end":1204,"source":"text"}]
abc123    VERSION       1.2.3                                        [{"start":2100,"end":2105,"source":"text"}]
abc123    EMAIL         author@example.com                           [{"start":5500,"end":5518,"source":"text"}]
abc123    LANG          python                                       [{"start":780,"end":790,"source":"text"}]
```

### 2. Preprocessed Text (`text-preprocessed/{doc_id}.txt`)

Plain text extracted from HTML with boilerplate removed:

- **Removes**: Navigation, headers, footers, sidebars, scripts, styles, GitHub chrome
- **Preserves**: Main content, code blocks, README sections
- **Cleans**: HTML entities unescaped, whitespace normalized, minified lines dropped
- **Format**: UTF-8 encoded, one file per HTML document

Directory structure mirrors input HTML structure.

### 3. README Files (`readme/{doc_id}.txt`)

When a README section is detected, a dedicated file is created in the `workspace/store/readme/` directory containing only the README content. Useful for dedicated README analysis.

- **Location**: `workspace/store/readme/` (separate from preprocessed text)
- **Format**: Plain text, UTF-8 encoded
- **Naming**: `{doc_id}.txt` (mirrors input structure)

## Architecture

### Modules

```
extractor/
├── __init__.py
├── __main__.py              # Existing simple text extractor
├── extractor.py             # Existing HtmlTextExtractor
├── regexes.py               # ★ Compiled regex patterns
├── html_clean.py            # ★ Boilerplate removal & text conversion
├── normalize.py             # ★ Value normalization (1.2k → 1200, etc.)
├── entity_extractors.py     # ★ Entity extraction functions
├── io_utils.py              # ★ File I/O helpers
└── entity_main.py           # ★ Main CLI orchestrator
```

★ = New modules added for entity extraction

### Data Flow

```
HTML File
   ↓
html_clean.strip_boilerplate_html()  →  Boilerplate-free HTML
   ↓
html_clean.html_to_text()            →  Preprocessed Text
   ↓                                      ↓
   ↓                                      ↓
entity_extractors.extract_all_entities() (uses both HTML & Text)
   ↓
Entity Rows (doc_id, type, value, offsets_json)
   ↓
TSVWriter.write_rows()               →  entities.tsv
```

### Regex Patterns

All patterns are pre-compiled in `regexes.py` for performance. Patterns use:

- **Word boundaries** (`\b`) to reduce false positives
- **Multiline mode** (`re.M`) for line-based patterns
- **Dotall mode** (`re.S`) for block patterns spanning lines
- **Case-insensitive** (`re.I`) where appropriate
- **Defensive matching** to handle malformed HTML

### HTML Preprocessing

`html_clean.py` implements a two-stage approach:

1. **Boilerplate Removal**:
   - Remove `<script>`, `<style>`, `<iframe>`, etc.
   - Remove GitHub-specific chrome (navigation, headers, footers)
   - Remove sidebars, breadcrumbs, command bars
   - Iteratively apply patterns to handle nesting

2. **Text Conversion**:
   - Convert block tags to newlines (`<p>`, `<div>`, `<h1>`, etc.)
   - Remove all remaining tags
   - Unescape HTML entities
   - Normalize whitespace
   - Drop minified lines (> 5000 chars)
   - Drop lines with no alphanumeric content

## Testing

### Smoke Test

```bash
# Run smoke test on samples
python tests/smoke_test.py

# Custom sample directory
python tests/smoke_test.py --samples /path/to/samples
```

**Smoke test validates**:
- ✓ Entities TSV created with correct header
- ✓ At least some entities extracted
- ✓ Preprocessed text files created
- ✓ No crashes on sample files
- ✓ Entity type distribution is reasonable

### Manual Testing

```bash
# Copy a few HTML files to test directory
mkdir -p tests_regex_samples
cp workspace/store/html/00/00/*.html tests_regex_samples/

# Run extraction
python -m extractor.entity_main --sample 5 --verbose

# Inspect outputs
head workspace/store/entities/entities.tsv
ls workspace/store/text-preprocessed/
```

## Performance

- **Speed**: ~25-30 files/second on typical hardware (depends on file size)
- **Memory**: Processes files one at a time (constant memory usage)
- **Disk I/O**: Streaming writes to TSV (no buffering all entities in memory)

### Optimization Tips

1. Use `--sample N` to test on a subset before full run:
   ```bash
   python -m extractor --sample 100
   ```

2. Run with `--verbose` to monitor progress:
   ```bash
   python -m extractor --verbose
   ```

3. Pre-compiled patterns are reused across files
4. TSV is written incrementally (not all at once)

## Troubleshooting

### No entities extracted

**Cause**: HTML structure doesn't match expected patterns.

**Solutions**:
- Run with `--verbose` to see debug logs
- Inspect sample HTML files manually
- Update regex patterns in `regexes.py` based on actual HTML structure

### Preprocessed text is empty

**Cause**: Boilerplate removal too aggressive.

**Solutions**:
- Check if HTML has unusual structure
- Temporarily disable boilerplate removal: `html_to_text(html, strip_boilerplate=False)`
- Adjust patterns in `html_clean.py`

### TSV file has invalid rows

**Cause**: Entity values contain tabs or newlines.

**Solutions**:
- TSV writer in `io_utils.py` escapes tabs/newlines by replacing with spaces
- For complex values (like `LANG_STATS`), use JSON encoding

### Crashes on specific files

**Cause**: Malformed HTML or encoding issues.

**Solutions**:
- Files are read with `errors='ignore'` by default
- All extraction functions have exception handling
- Check logs for specific error messages

## Extending

### Adding New Entity Types

1. **Define regex pattern** in `regexes.py`:
   ```python
   _MY_ENTITY_RE = re.compile(r'pattern here', re.MULTILINE)

   def get_my_entity_regex():
       return _MY_ENTITY_RE
   ```

2. **Create extractor function** in `entity_extractors.py`:
   ```python
   def extract_my_entities(doc_id: str, text_content: str) -> List[EntityRow]:
       results = []
       pattern = regexes.get_my_entity_regex()

       for match in pattern.finditer(text_content):
           value = match.group(1).strip()
           offsets = [{
               'start': match.start(),
               'end': match.end(),
               'source': 'text'
           }]

           results.append((
               doc_id,
               'MY_ENTITY',
               value,
               json.dumps(offsets, separators=(',', ':'))
           ))

       return results
   ```

3. **Add to `extract_all_entities()`**:
   ```python
   results.extend(extract_my_entities(doc_id, text_content))
   ```

### Custom Boilerplate Removal

Edit `GITHUB_CHROME_PATTERNS` in `html_clean.py`:

```python
GITHUB_CHROME_PATTERNS = [
    re.compile(r'<div[^>]*?class="my-custom-chrome"[^>]*?>.*?</div>', re.IGNORECASE | re.DOTALL),
    # ... more patterns
]
```

## Limitations

1. **Regex-only**: No DOM parsing means we rely on text patterns. Complex nested structures may be missed.

2. **GitHub-specific**: Patterns are calibrated for GitHub HTML. Other sites need pattern adjustments.

3. **Offset precision**: Offsets are character positions in raw HTML/text. Useful for debugging but not byte-accurate for binary formats.

4. **No language-aware parsing**: Import extraction is purely pattern-based, may miss complex syntax.

5. **Best-effort matching**: Missing data results in empty fields, not errors. Robust but may silently skip content.

## License

This extractor inherits the project license. Uses only Python standard library (no external dependencies).

## Contact

For issues or questions, refer to the main project documentation or open an issue in the project repository.

---

**Generated**: 2025-10-29
**Python Version**: 3.7+
**Dependencies**: None (stdlib only)
