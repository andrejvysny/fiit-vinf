"""HTML boilerplate removal and text extraction using regex only.

This module provides functions to clean GitHub HTML pages by removing
navigation, scripts, styles, and other chrome elements, then converting
to plain text without DOM parsing.
"""

import html
from typing import Pattern

from spark.lib.extractor import regexes


# Minified code detection (single lines longer than this are likely minified)
MAX_LINE_LENGTH = 5000


def strip_iteratively(text: str, pattern: Pattern[str]) -> str:
    """Apply a regex pattern repeatedly until no more matches."""
    max_iterations = 100
    for _ in range(max_iterations):
        text, count = pattern.subn(' ', text)
        if count == 0:
            break
    return text


def remove_non_content_tags(html_content: str) -> str:
    """Remove script/style blocks and metadata tags."""
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
    """Remove GitHub-specific boilerplate and navigation chrome from HTML."""
    if not html_content:
        return ''

    working = remove_non_content_tags(html_content)
    chrome_patterns = regexes.get_html_chrome_patterns()

    for pattern in chrome_patterns:
        working = strip_iteratively(working, pattern)

    return working


def html_to_text(html_content: str, strip_boilerplate: bool = True) -> str:
    """Convert HTML to plain text using regex only."""
    if not html_content:
        return ''

    working = html_content

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

    working = block_tags_re.sub('\n', working)
    working = br_hr_re.sub('\n', working)
    working = generic_tag_re.sub(' ', working)
    working = html.unescape(working)
    working = working.replace('\xa0', ' ')
    working = working.replace('\r\n', '\n').replace('\r', '\n')
    working = multi_space_re.sub(' ', working)
    working = around_newline_re.sub('\n', working)

    lines = working.split('\n')
    cleaned_lines = []

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if len(line) > MAX_LINE_LENGTH:
            continue
        if not any(c.isalnum() for c in line):
            continue
        cleaned_lines.append(line)

    text = '\n'.join(cleaned_lines)
    text = multi_newline_re.sub('\n\n', text)

    return text.strip()
