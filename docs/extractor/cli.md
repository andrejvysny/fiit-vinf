## Command-line usage (HtmlTextExtractor)

The extractor is implemented as a small CLI. The interface is defined by `parse_args` in `extractor/__init__.py`.

Default behaviour

- Reads HTML files from `workspace/store/html` by default.
- Writes extracted plain text files into `workspace/store/text` by mirroring input file paths and changing the suffix to `.txt`.

Options

- `--input-root` (default: `workspace/store/html`)
  - Root directory to search for HTML files. If you pass explicit file paths via `--file` or `--files-list`, those are resolved relative to this root when they are not absolute.

- `--output-root` (default: `workspace/store/text`)
  - Destination directory where `.txt` files are written. The extractor mirrors the input directory structure.

- `--files-list`
  - Path to a text file that lists HTML files (one per line). Paths in the list may be absolute or relative to `--input-root`.

- `--file` (repeatable)
  - Explicit HTML file path to process. Can be provided multiple times.

- `--limit` (int)
  - Maximum number of files to process after filtering. Use `0` or negative to disable.

- `--force`
  - Re-extract even when the target `.txt` already exists.

- `--dry-run`
  - Print the files that would be processed and exit without writing outputs.

- `--verbose`
  - Enable debug logging (helps when diagnosing why a file was skipped or why a regex removed content you expected to keep).

Behaviour notes

- When invoked without `--file` or `--files-list`, the extractor recursively finds `*.html` files under `--input-root`.
- The CLI uses the `extract_file` function which mirrors the input tree and creates parent directories as needed. Files that already have an extracted `.txt` are skipped unless `--force` is used.
- Errors while extracting an individual file are logged and the extractor continues with the next file.
