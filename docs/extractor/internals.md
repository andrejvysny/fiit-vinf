## Extraction pipeline (HtmlTextExtractor)

This document describes the step-by-step processing pipeline used by the extractor in `extractor/__init__.py`.

High-level contract

- Input: a string containing HTML content (UTF-8). The extractor tolerates invalid characters and decodes with `errors='ignore'` when reading files.
- Output: `TextExtractionResult` with two fields: `title` (string) and `text` (string). `text` is a cleaned plain-text representation of the HTML body.
- Error modes: the extractor returns an empty result for empty input. File I/O errors are raised at the caller level; the CLI logs exceptions and continues.

Pipeline steps

1. Title extraction

   - The extractor searches for the first `<title>...</title>` occurrence using a case-insensitive, dot-all regex and extracts the inner text before other transformations. HTML entities are unescaped and multiple spaces are collapsed.

2. Boilerplate and block removal

   The extractor removes large structural and non-content parts of the document early. Patterns are applied iteratively where appropriate.

   - `<head>...</head>` block is removed entirely (first via `_HEAD_RE`).
   - Drop-block tags like `<script>`, `<style>`, `<iframe>`, `<canvas>`, `<svg>`, `<noscript>`, `<template>`, `<form>`, `<dialog>` are removed together (via `_DROP_BLOCK_TAGS_RE`). These blocks can contain large amounts of non-content or markup.
   - Navigation-like structures such as `<header>`, `<footer>`, `<nav>`, `<aside>` are removed (via `_NAV_LIKE_TAGS_RE`).
   - Tags containing attributes like `class` or `id` that include specific keywords (e.g. `header`, `nav`, `sidebar`, `pagination`) are removed as complete elements (see `_ATTRIBUTE_KEYWORD_RE` in `regexes.md`).

3. Metadata and comments

   - Self-closing metadata tags such as `<meta>`, `<link>`, `<base>`, `<input>` are replaced with a single space.
   - HTML comments (`<!-- ... -->`) and the `<!DOCTYPE ...>` declaration are removed.

4. Convert HTML separators to newlines

   - `<br>` elements and horizontal rules (`<hr>`) are converted to newline characters.
   - Block-level structural tags (paragraphs, headings, divs, sections, tables, list items etc.) are replaced with newlines so blocks appear as separate paragraphs in the output.

5. Remove all remaining tags

   - Any remaining `<...>` tags are stripped leaving only text.

6. Unescape entities and normalize whitespace

   - HTML entities are unescaped (for example `&amp;` → `&`). Non-breaking spaces are replaced with normal spaces.
   - Windows and old Mac newlines (`\r\n`, `\r`) are normalized to `\n`.
   - Multiple horizontal spaces are collapsed. Surrounding spaces around single newlines are trimmed and runs of 3+ newlines collapse down to two consecutive newlines.

7. Post-filtering lines

   - The cleaned text is split into lines, each line is trimmed and then filtered using `_should_keep_line`:
     - Empty lines are dropped.
     - Lines that appear in a denylist (common navigation labels such as "Sign in", "Pricing", "Issues", "Pull requests", "GitHub" etc.) are dropped.
     - Lines that contain no alphanumeric characters are dropped.

8. Return result

   - `title` is returned as extracted earlier, `text` is the joined set of kept lines separated by a single `\n` (paragraphs separated by blank lines remain as `\n\n`).

Notes on iterative stripping

- Some regexes (for example `_HEAD_RE`, `_DROP_BLOCK_TAGS_RE`, `_NAV_LIKE_TAGS_RE`, `_ATTRIBUTE_KEYWORD_RE`) are applied in a loop until they no longer match. This helps remove nested occurrences or repeated structures that a single pass would miss.
