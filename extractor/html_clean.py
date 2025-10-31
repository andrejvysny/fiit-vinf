"""HTML boilerplate removal and text extraction using regex only.

This module provides functions to clean GitHub HTML pages by removing
navigation, scripts, styles, and other chrome elements, then converting
to plain text without DOM parsing.
"""

import html
from typing import Pattern, Tuple

from extractor import regexes


# Minified code detection (single lines longer than this are likely minified)
MAX_LINE_LENGTH = 5000


def strip_iteratively(text: str, pattern: Pattern[str]) -> str:
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


def remove_non_content_tags(html_content: str) -> str:
    """Remove script/style blocks and metadata tags that are never user-facing."""
    if not html_content:
        return ''

    working = html_content
    remove_tags_re = regexes.get_html_remove_tags_regex()
    comment_re = regexes.get_html_comment_regex()
    doctype_re = regexes.get_html_doctype_regex()
    self_closing_re = regexes.get_html_self_closing_regex()

    working = strip_iteratively(working, remove_tags_re)
    working = comment_re.sub(' ', working)
    working = doctype_re.sub(' ', working)
    working = self_closing_re.sub(' ', working)

    return working


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

    working = remove_non_content_tags(html_content)
    chrome_patterns = regexes.get_html_chrome_patterns()

    # Remove GitHub-specific chrome patterns
    for pattern in chrome_patterns:
        working = strip_iteratively(working, pattern)

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

    # Step 1: Remove boilerplate if requested, otherwise strip only non-content tags
    if strip_boilerplate:
        working = strip_boilerplate_html(working)
    else:
        working = remove_non_content_tags(working)

    block_tags_re = regexes.get_html_block_tags_regex()
    br_hr_re = regexes.get_html_br_hr_regex()
    generic_tag_re = regexes.get_html_generic_tag_regex()
    multi_space_re = regexes.get_html_multi_space_regex()
    around_newline_re = regexes.get_html_around_newline_regex()
    multi_newline_re = regexes.get_html_multi_newline_regex()

    # Step 2: Convert block-level tags to newlines
    working = block_tags_re.sub('\n', working)
    working = br_hr_re.sub('\n', working)

    # Step 3: Remove all remaining tags
    working = generic_tag_re.sub(' ', working)

    # Step 4: Unescape HTML entities
    working = html.unescape(working)

    # Step 5: Normalize special characters
    working = working.replace('\xa0', ' ')  # Non-breaking space
    working = working.replace('\r\n', '\n').replace('\r', '\n')

    # Step 6: Normalize whitespace
    working = multi_space_re.sub(' ', working)
    working = around_newline_re.sub('\n', working)

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
    text = multi_newline_re.sub('\n\n', text)

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
