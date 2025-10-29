## Regex reference and explanation

This file documents each regular expression used by the extractor in `extractor/__init__.py`. Where helpful, examples are included.

General notes about flags used

- `(?i)` — case-insensitive matching.
- `(?s)` — dot-all: `.` matches newlines as well.
- Patterns are often combined like `(?is)` to enable both behaviors.

Regex list

1. `_TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")`

   - What it matches: the content between the first `<title ...>` and `</title>` tag (non-greedy). The `[^>]*` allows attributes on the `<title>` tag.
   - Flags: case-insensitive and dot-all so that the title is found even if it spans lines or has mixed-case tags.
   - Example: `<title>My &amp; Project</title>` → capture group 1 is `My &amp; Project`.

2. `_HEAD_RE = re.compile(r"(?is)<head\b[^>]*>.*?</head>)")`

   - What it matches: the entire `<head>...</head>` section, including attributes on `<head>` and everything inside.
   - Why it's removed: head contains scripts, meta tags, styles, and other non-visible information that do not contribute to readable body text.

3. `_DROP_BLOCK_TAGS_RE = re.compile(r"(?is)<(script|style|iframe|canvas|svg|noscript|template|form|dialog)[^>]*>.*?</\1>")`

   - What it matches: any of the named tags above and their full content. Uses a capturing group for the tag name and `</\1>` to match the corresponding closing tag.
   - Why: These elements typically hold code, styling, or non-textual content.

4. `_NAV_LIKE_TAGS_RE = re.compile(r"(?is)<(header|footer|nav|aside)[^>]*>.*?</\1>")`

   - What it matches: site navigation and similar page chrome sections.
   - Why: These often contain repeated navigation links and menus that pollute the extracted content.

5. `_COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)`

   - What it matches: HTML comments (`<!-- comment -->`). Uses DOTALL so comments spanning multiple lines are matched.

6. `_DOCTYPE_RE = re.compile(r"<!DOCTYPE.*?>", re.IGNORECASE | re.DOTALL)`

   - What it matches: the doctype declaration (for example `<!DOCTYPE html>`). It is removed entirely.

7. `_SELF_CLOSING_METADATA_RE = re.compile(r"(?is)<(meta|link|base|input)[^>]*?>")`

   - What it matches: common self-closing or void tags that are metadata or non-content (meta, link, base, input). These are replaced by a single space to avoid word merges.

8. `_BLOCK_BREAK_RE = re.compile(
    r"</?(?:p|div|section|article|main|h[1-6]|pre|blockquote|li|ul|ol|table|tr|thead|tbody|tfoot|th|td|dl|dt|dd)[^>]*>",
    re.IGNORECASE,
)
`

   - What it matches: opening or closing block-level tags that usually indicate paragraph or section boundaries.
   - What it does: replaced with a single newline character so that logical blocks remain separated in the text output.

9. `_BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)` and `_HR_RE = re.compile(r"<hr\s*/?>", re.IGNORECASE)`

   - What they match: `<br>`, `<br/>`, `<hr>`, `<hr/>` (case-insensitive). Replaced with `\n` to preserve line breaks.

10. `_TAG_RE = re.compile(r"<[^>]+>")`

   - What it matches: any remaining tags of the form `<...>` that haven't been removed earlier.
   - What it does: strips them, leaving only text content.

11. `_MULTI_SPACE_RE = re.compile(r"[ \t\f\v]+")`

   - What it matches: runs of spaces and horizontal whitespace (tabs and similar). Collapsed to a single space.

12. `_AROUND_NEWLINE_SPACE_RE = re.compile(r"[ \t\f\v]*\n[ \t\f\v]*")`

   - What it matches: optional spaces surrounding a single newline. Replaces them with a single newline, trimming surrounding spaces on line breaks.

13. `_MULTI_NEWLINE_RE = re.compile(r"\n{3,}")`

   - What it matches: three or more consecutive newlines. Collapses them to two newlines (i.e. a single blank-line separation between paragraphs).

14. `_ATTRIBUTE_KEYWORD_RE = re.compile(
    r"(?is)<(?P<tag>\w+)(?P<attrs>[^>]*?(?:class|id)\s*=\s*['\"][^'\"]*(?:header|footer|nav|menu|toolbar|sidebar|breadcrumbs|pagination|command-bar|filter-bar)[^'\"]*['\"][^>]*)>.*?</\1>"
)

   - What it matches: an element where the `class` or `id` attribute contains any of a set of keywords commonly used for navigation or UI chrome (for example `class="site-header"` or `id="sidebar"`). The entire element is removed.
   - Why: not all navigation or sidebar blocks use semantic tags like `<nav>` or `<aside>`. Many pages use generic `<div>`s with classes/ids; this regex attempts to catch those.

15. `_LINE_DENYLIST` (set of strings)

   - This is not a regex but a Python set of lowercase strings. After cleaning, any line that matches one of these strings exactly (case-insensitive) is dropped. Examples: `"sign in"`, `"pricing"`, `"issues"`, `"pull requests"`, `"github"`.

16. `_MULTI_NEWLINE_RE` and related normalization

   - After tag removal and whitespace collapsing, the extractor also normalizes newline sequences and trims extra spaces around newlines so that the final text is readable and compact.

Examples (mini):

- Input fragment:

```html
<header><nav>Sign in | Pricing</nav></header>
<main>
  <h1>Example</h1>
  <p>This is <strong>content</strong>.</p>
  <script>var a = 1;</script>
</main>
```

- After `_NAV_LIKE_TAGS_RE` and `_DROP_BLOCK_TAGS_RE`: `<main>\n  <h1>Example</h1>\n  <p>This is <strong>content</strong>.</p>\n\n</main>`
- After `_BLOCK_BREAK_RE` and `_TAG_RE` and whitespace normalization: `Example\n\nThis is content.`
