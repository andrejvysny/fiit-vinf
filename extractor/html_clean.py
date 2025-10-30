"""HTML boilerplate removal and text extraction using regex only.

This module provides functions to clean GitHub HTML pages by removing
navigation, scripts, styles, and other chrome elements, then converting
to plain text without DOM parsing.
"""

import html
import re
from typing import Tuple


# GitHub-specific chrome patterns (derived from sample HTML inspection)
# These patterns target common navigation and UI elements
GITHUB_CHROME_PATTERNS = [
    # Skip to content link
    re.compile(r'<a[^>]*?href="#[^"]*?skip[^"]*?"[^>]*?>.*?</a>', re.IGNORECASE | re.DOTALL),

    # Global header and navigation
    re.compile(r'<header[^>]*?class="[^"]*?(?:Header|AppHeader|header)[^"]*?"[^>]*?>.*?</header>', re.IGNORECASE | re.DOTALL),

    # Navigation bars
    re.compile(r'<nav[^>]*?(?:class="[^"]*?(?:UnderlineNav|navigation|nav-bar)[^"]*?"|role="navigation")[^>]*?>.*?</nav>', re.IGNORECASE | re.DOTALL),

    # Sidebar elements
    re.compile(r'<(?:div|aside)[^>]*?class="[^"]*?(?:sidebar|Sidebar)[^"]*?"[^>]*?>.*?</(?:div|aside)>', re.IGNORECASE | re.DOTALL),

    # Footer
    re.compile(r'<footer[^>]*?>.*?</footer>', re.IGNORECASE | re.DOTALL),

    # Breadcrumbs
    re.compile(r'<nav[^>]*?aria-label="[^"]*?[Bb]readcrumb[^"]*?"[^>]*?>.*?</nav>', re.IGNORECASE | re.DOTALL),

    # Command palette and filters
    re.compile(r'<div[^>]*?class="[^"]*?(?:command-bar|filter-bar|CommandBar)[^"]*?"[^>]*?>.*?</div>', re.IGNORECASE | re.DOTALL),

    # Pagination
    re.compile(r'<div[^>]*?class="[^"]*?pagination[^"]*?"[^>]*?>.*?</div>', re.IGNORECASE | re.DOTALL),
]

# Block-level elements to remove entirely
REMOVE_TAGS_RE = re.compile(
    r'<(script|style|iframe|canvas|svg|noscript|template|form|dialog|input|button|select|textarea)[^>]*?>.*?</\1>',
    re.IGNORECASE | re.DOTALL
)

# Self-closing tags to remove
SELF_CLOSING_RE = re.compile(
    r'<(?:meta|link|base|input|img|br|hr|area|embed|param|source|track|wbr)[^>]*?/?>',
    re.IGNORECASE
)

# HTML comments
COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)

# DOCTYPE
DOCTYPE_RE = re.compile(r'<!DOCTYPE[^>]*?>', re.IGNORECASE | re.DOTALL)

# Block-level tags that should become newlines
BLOCK_TAGS_RE = re.compile(
    r'</?(?:p|div|section|article|main|h[1-6]|pre|blockquote|li|ul|ol|'
    r'table|tr|thead|tbody|tfoot|th|td|dl|dt|dd|address|figcaption|figure|'
    r'header|footer|aside|nav)[^>]*?>',
    re.IGNORECASE
)

# BR and HR tags
BR_HR_RE = re.compile(r'<(?:br|hr)\s*/?>', re.IGNORECASE)

# Generic tag remover (catch-all for remaining tags)
TAG_RE = re.compile(r'<[^>]+>')

# Whitespace normalization patterns
MULTI_SPACE_RE = re.compile(r'[ \t\f\v]+')
AROUND_NEWLINE_RE = re.compile(r'[ \t\f\v]*\n[ \t\f\v]*')
MULTI_NEWLINE_RE = re.compile(r'\n{3,}')

# Minified code detection (single lines longer than this are likely minified)
MAX_LINE_LENGTH = 5000


def strip_iteratively(text: str, pattern: re.Pattern) -> str:
    """Apply a regex pattern repeatedly until no more matches.

    This is necessary for nested structures like <nav><nav>...</nav></nav>
    which might not be caught in a single pass.
    """
    max_iterations = 100  # Safety limit
    for _ in range(max_iterations):
        text, count = pattern.subn(' ', text)
        if count == 0:
            break
    return text


def strip_boilerplate_html(html_content: str) -> str:
    """Remove GitHub-specific boilerplate and navigation chrome from HTML.

    This function removes:
    - <script>, <style>, <iframe>, and other non-content tags
    - GitHub navigation bars, headers, footers
    - Sidebar elements
    - Breadcrumbs and command bars
    - Comments and metadata tags

    Returns HTML with boilerplate removed but tags still intact.

    Args:
        html_content: Raw HTML string

    Returns:
        Cleaned HTML string with boilerplate removed
    """
    if not html_content:
        return ''

    working = html_content

    # Remove scripts, styles, and other non-content blocks
    working = strip_iteratively(working, REMOVE_TAGS_RE)

    # Remove GitHub-specific chrome patterns
    for pattern in GITHUB_CHROME_PATTERNS:
        working = strip_iteratively(working, pattern)

    # Remove comments and DOCTYPE
    working = COMMENT_RE.sub(' ', working)
    working = DOCTYPE_RE.sub(' ', working)

    # Remove self-closing metadata tags
    working = SELF_CLOSING_RE.sub(' ', working)

    return working


def html_to_text(html_content: str, strip_boilerplate: bool = True) -> str:
    """Convert HTML to plain text using regex only.

    Process:
    1. Optionally strip boilerplate (navigation, chrome)
    2. Convert block tags to newlines
    3. Remove all remaining tags
    4. Unescape HTML entities
    5. Normalize whitespace
    6. Remove minified code blobs (lines > MAX_LINE_LENGTH)
    7. Remove lines with no alphanumeric content

    Args:
        html_content: Raw HTML string
        strip_boilerplate: If True, remove GitHub chrome before conversion

    Returns:
        Plain text string
    """
    if not html_content:
        return ''

    working = html_content

    # Step 1: Remove boilerplate if requested
    if strip_boilerplate:
        working = strip_boilerplate_html(working)

    # Step 2: Convert block-level tags to newlines
    working = BLOCK_TAGS_RE.sub('\n', working)
    working = BR_HR_RE.sub('\n', working)

    # Step 3: Remove all remaining tags
    working = TAG_RE.sub(' ', working)

    # Step 4: Unescape HTML entities
    working = html.unescape(working)

    # Step 5: Normalize special characters
    working = working.replace('\xa0', ' ')  # Non-breaking space
    working = working.replace('\r\n', '\n').replace('\r', '\n')

    # Step 6: Normalize whitespace
    working = MULTI_SPACE_RE.sub(' ', working)
    working = AROUND_NEWLINE_RE.sub('\n', working)

    # Step 7: Split into lines and filter
    lines = working.split('\n')
    cleaned_lines = []

    for line in lines:
        line = line.strip()

        # Skip empty lines
        if not line:
            continue

        # Skip minified code blobs (very long lines)
        if len(line) > MAX_LINE_LENGTH:
            continue

        # Skip lines with no alphanumeric content
        if not any(c.isalnum() for c in line):
            continue

        cleaned_lines.append(line)

    text = '\n'.join(cleaned_lines)

    # Final whitespace normalization
    text = MULTI_NEWLINE_RE.sub('\n\n', text)

    return text.strip()


def extract_text_with_offsets(html_content: str) -> Tuple[str, str]:
    """Extract both raw text and preprocessed text, useful for offset tracking.

    Returns:
        Tuple of (preprocessed_text, raw_text_no_tags)
    """
    # Preprocessed version (with boilerplate removal)
    preprocessed = html_to_text(html_content, strip_boilerplate=True)

    # Raw version (just tags removed, no boilerplate filtering)
    raw_text = html_to_text(html_content, strip_boilerplate=False)

    return preprocessed, raw_text
