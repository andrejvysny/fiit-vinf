"""Compiled regex patterns for entity extraction.

All patterns are pre-compiled for performance. This module uses only stdlib `re`.
Patterns are designed to be defensive with word boundaries and negative lookaheads
to reduce false positives.
"""

import re
from typing import Dict, List, Pattern

# ============================================================================
# HTML Cleaning Patterns
# ============================================================================

_HTML_CHROME_PATTERNS: List[Pattern] = [
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
    # More specific: match up to the next closing </div> without spanning unrelated tags
    re.compile(r'<div[^>]*?class="[^"]*?pagination[^"]*?"[^>]*?>[\s\S]*?</div>', re.IGNORECASE),
]

_HTML_REMOVE_TAGS_RE = re.compile(
    r'<(script|style|iframe|canvas|svg|noscript|template|form|dialog|input|button|select|textarea)[^>]*?>.*?</\1>',
    re.IGNORECASE | re.DOTALL
)

_HTML_SELF_CLOSING_RE = re.compile(
    r'<(?:meta|link|base|input|img|br|hr|area|embed|param|source|track|wbr)[^>]*?/?>',
    re.IGNORECASE
)

_HTML_COMMENT_RE = re.compile(r'<!--.*?-->', re.DOTALL)
_HTML_DOCTYPE_RE = re.compile(r'<!DOCTYPE[^>]*?>', re.IGNORECASE | re.DOTALL)

_HTML_BLOCK_TAGS_RE = re.compile(
    r'</?(?:p|div|section|article|main|h[1-6]|pre|blockquote|li|ul|ol|'
    r'table|tr|thead|tbody|tfoot|th|td|dl|dt|dd|address|figcaption|figure|'
    r'header|footer|aside|nav)[^>]*?>',
    re.IGNORECASE
)

_HTML_BR_HR_RE = re.compile(r'<(?:br|hr)\s*/?>', re.IGNORECASE)
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_HTML_MULTI_SPACE_RE = re.compile(r'[ \t\f\v]+')
_HTML_AROUND_NEWLINE_RE = re.compile(r'[ \t\f\v]*\n[ \t\f\v]*')
_HTML_MULTI_NEWLINE_RE = re.compile(r'\n{3,}')


# ============================================================================
# GitHub Metadata Patterns
# ============================================================================

# Stars counter patterns
# Example: <span id="repo-stars-counter-star" title="34,222">34.2k</span>
_STAR_COUNTER_RE = re.compile(
    r'<span\s+id="repo-stars-counter-star"[^>]*?title="([0-9,]+)"',
    re.IGNORECASE | re.DOTALL
)

# Alternative star pattern from stargazers link
# Example: <a href="/owner/repo/stargazers">...34.2k</a>
_STAR_ARIA_RE = re.compile(
    r'aria-label="(\d+(?:,\d+)*)\s+users?\s+starred',
    re.IGNORECASE
)

# Forks counter patterns
# Example: <span id="repo-network-counter" title="12,790">12.8k</span>
_FORK_COUNTER_RE = re.compile(
    r'<span\s+id="repo-network-counter"[^>]*?title="([0-9,]+)"',
    re.IGNORECASE | re.DOTALL
)

# Language stats patterns
# Matches: <span itemprop="programmingLanguage">Python</span>
_LANG_NAME_RE = re.compile(
    r'<span\s+[^>]*?itemprop="programmingLanguage"[^>]*?>([^<]+)</span>',
    re.IGNORECASE | re.DOTALL
)

# Language percentage pattern
# Matches percentage values near language names, e.g., "73.5%"
_LANG_PERCENT_RE = re.compile(
    r'(\d{1,3}(?:\.\d+)?)\s*%',
    re.MULTILINE
)

# Full language bar extraction (finds entire language stats section)
# Looks for the language bar container with multiple languages
_LANG_STATS_BLOCK_RE = re.compile(
    r'<div[^>]*?(?:class="[^"]*?language[^"]*?"|itemprop="programmingLanguage")[^>]*?>.*?</div>',
    re.IGNORECASE | re.DOTALL
)

# ============================================================================
# README Extraction Patterns
# ============================================================================

# README markdown article container
# GitHub wraps README in <article> with specific classes/attributes
_README_ARTICLE_RE = re.compile(
    r'<article[^>]*?(?:class="[^"]*?markdown-body[^"]*?"|itemprop="text"|data-[^>]*?readme)[^>]*?>(.*?)</article>',
    re.IGNORECASE | re.DOTALL
)

# Alternative: README in JSON payloads (application/json script blocks)
_README_JSON_SCRIPT_RE = re.compile(
    r'<script\s+[^>]*?type="application/json"[^>]*?>(.*?)</script>',
    re.IGNORECASE | re.DOTALL
)

# About section extraction
_ABOUT_HEADER_RE = re.compile(
    r'<h2[^>]*?>\s*About\s*</h2>(.*?)(?=<h[12]|<div[^>]*?class="[^"]*?(?:sidebar|footer)[^"]*?")',
    re.IGNORECASE | re.DOTALL
)

# ============================================================================
# License Patterns
# ============================================================================

# SPDX license identifiers and common licenses
# Matches: MIT, Apache-2.0, GPL-3.0, BSD-3-Clause, etc.
_LICENSE_SPDX_RE = re.compile(
    r'\b(MIT|Apache-2\.0|Apache-1\.1|GPL-3\.0|GPL-2\.0|LGPL-3\.0|LGPL-2\.1|'
    r'BSD-3-Clause|BSD-2-Clause|MPL-2\.0|ISC|CC0-1\.0|Unlicense|'
    r'AGPL-3\.0|EPL-2\.0|EPL-1\.0|CC-BY-4\.0|CC-BY-SA-4\.0)\b',
    re.IGNORECASE
)

# License file links and badges
_LICENSE_LINK_RE = re.compile(
    r'<a\s+[^>]*?href="[^"]*?(?:license|LICENSE)[^"]*?"[^>]*?>(.*?)</a>',
    re.IGNORECASE | re.DOTALL
)

# ============================================================================
# Topic/Tag Patterns
# ============================================================================

# GitHub repository topics
# Example: <a data-octo-click="topic" class="topic-tag">machine-learning</a>
_TOPIC_TAG_RE = re.compile(
    r'<a\s+[^>]*?(?:class="[^"]*?topic-tag[^"]*?"|data-octo-click="topic")[^>]*?>([\w\-]+)</a>',
    re.IGNORECASE | re.DOTALL
)

# ============================================================================
# Import Statement Patterns (code-level)
# ============================================================================

# Python imports
# Matches: import foo, from foo import bar, from foo.bar import baz as qux
_IMPORT_PYTHON_RE = re.compile(
    r'^\s*(?:import\s+([\w\.]+(?:\s*,\s*[\w\.]+)*)|from\s+([\w\.]+)\s+import\s+([\w\*]+(?:\s*,\s*[\w\*]+)*))',
    re.MULTILINE
)

# JavaScript/TypeScript imports
# Matches: import foo from "bar", require("foo")
_IMPORT_JS_RE = re.compile(
    r'(?:^\s*import\s+.*?\s+from\s+[\'"]([^\'"]+)[\'"]|require\s*\(\s*[\'"]([^\'"]+)[\'"]\s*\))',
    re.MULTILINE
)

# Rust use statements
# Matches: use std::collections::HashMap;
_IMPORT_RUST_RE = re.compile(
    r'^\s*use\s+([\w:{}]+(?:\s*,\s*[\w:{}]+)*)\s*;',
    re.MULTILINE
)

# C/C++ includes
# Matches: #include <stdio.h>, #include "myheader.h"
_IMPORT_C_RE = re.compile(
    r'^\s*#\s*include\s*[<"]([^>"]+)[>"]',
    re.MULTILINE
)

# PHP use statements
# Matches: use Vendor\Package\Class;
_IMPORT_PHP_RE = re.compile(
    r'^\s*use\s+([\w\\]+)\s*;',
    re.MULTILINE
)

# Go imports
# Matches: import "fmt", import ( "fmt" "os" )
_IMPORT_GO_RE = re.compile(
    r'^\s*import\s+(?:"([^"]+)"|(\([^)]+\)))',
    re.MULTILINE
)

# ============================================================================
# URL Patterns
# ============================================================================

# HTTP/HTTPS URLs
# Matches both in text and in href attributes
_URL_HTTP_RE = re.compile(
    r'https?://[^\s<>"\']+',
    re.IGNORECASE
)

# GitHub-specific URLs in markdown links
_URL_MARKDOWN_LINK_RE = re.compile(
    r'\[([^\]]+)\]\(([^\)]+)\)',
    re.MULTILINE
)

# HTML anchor href
_URL_HTML_HREF_RE = re.compile(
    r'<a\s+[^>]*?href=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL
)

# ============================================================================
# Issue Reference Patterns
# ============================================================================

# GitHub issue references: #123, owner/repo#123, GH-123
_ISSUE_REF_RE = re.compile(
    r'(?:#(\d+)|([\w\-\.]+)/([\w\-\.]+)#(\d+)|GH-(\d+))\b',
    re.MULTILINE
)

# Issue and PR URLs
_ISSUE_URL_RE = re.compile(
    r'github\.com/([\w\-\.]+)/([\w\-\.]+)/(?:issues|pull)/(\d+)',
    re.IGNORECASE
)

# ============================================================================
# Version Patterns
# ============================================================================

# Semantic versioning: v1.2.3, 1.2.3-alpha, 1.0.0+build.123
_VERSION_SEMVER_RE = re.compile(
    r'\bv?(\d+\.\d+\.\d+(?:-[a-zA-Z0-9\.\-]+)?(?:\+[a-zA-Z0-9\.\-]+)?)\b',
    re.MULTILINE
)

# ============================================================================
# Email Patterns
# ============================================================================

# RFC-lite email pattern (defensive, not overly greedy)
_EMAIL_RE = re.compile(
    r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b',
    re.MULTILINE
)

# ============================================================================
# Code Language Hints (from code blocks)
# ============================================================================

# Fenced code blocks with language hints
# Matches: ```python, ```javascript, etc.
_CODE_FENCE_LANG_RE = re.compile(
    r'^```\s*([a-zA-Z0-9_+#\-]+)',
    re.MULTILINE
)

# ============================================================================
# Pattern Accessor Functions
# ============================================================================

def get_html_chrome_patterns() -> List[Pattern]:
    """Return compiled patterns targeting GitHub chrome sections."""
    return _HTML_CHROME_PATTERNS


def get_html_remove_tags_regex() -> Pattern:
    """Return pattern for removing non-content HTML tags."""
    return _HTML_REMOVE_TAGS_RE


def get_html_self_closing_regex() -> Pattern:
    """Return pattern for removing self-closing metadata tags."""
    return _HTML_SELF_CLOSING_RE


def get_html_comment_regex() -> Pattern:
    """Return pattern for stripping HTML comments."""
    return _HTML_COMMENT_RE


def get_html_doctype_regex() -> Pattern:
    """Return pattern for stripping DOCTYPE declarations."""
    return _HTML_DOCTYPE_RE


def get_html_block_tags_regex() -> Pattern:
    """Return pattern mapping block-level tags to newlines."""
    return _HTML_BLOCK_TAGS_RE


def get_html_br_hr_regex() -> Pattern:
    """Return pattern for replacing <br> and <hr> tags."""
    return _HTML_BR_HR_RE


def get_html_generic_tag_regex() -> Pattern:
    """Return catch-all pattern for stripping remaining tags."""
    return _HTML_TAG_RE


def get_html_multi_space_regex() -> Pattern:
    """Return pattern for collapsing multiple spaces."""
    return _HTML_MULTI_SPACE_RE


def get_html_around_newline_regex() -> Pattern:
    """Return pattern for normalizing whitespace around newlines."""
    return _HTML_AROUND_NEWLINE_RE


def get_html_multi_newline_regex() -> Pattern:
    """Return pattern for collapsing excessive blank lines."""
    return _HTML_MULTI_NEWLINE_RE


def get_star_regexes() -> List[Pattern]:
    """Return list of star count regex patterns (try in order)."""
    return [_STAR_COUNTER_RE, _STAR_ARIA_RE]


def get_fork_regexes() -> List[Pattern]:
    """Return list of fork count regex patterns."""
    return [_FORK_COUNTER_RE]


def get_lang_stats_regexes() -> Dict[str, Pattern]:
    """Return dict of language stats patterns."""
    return {
        'name': _LANG_NAME_RE,
        'percent': _LANG_PERCENT_RE,
        'block': _LANG_STATS_BLOCK_RE,
    }


def get_readme_regexes() -> Dict[str, Pattern]:
    """Return dict of README extraction patterns."""
    return {
        'article': _README_ARTICLE_RE,
        'json_script': _README_JSON_SCRIPT_RE,
        'about': _ABOUT_HEADER_RE,
    }


def get_license_regexes() -> List[Pattern]:
    """Return list of license detection patterns."""
    return [_LICENSE_SPDX_RE, _LICENSE_LINK_RE]


def get_topic_regex() -> Pattern:
    """Return topic/tag extraction pattern."""
    return _TOPIC_TAG_RE


def get_import_regexes() -> Dict[str, Pattern]:
    """Return dict of import statement patterns by language."""
    return {
        'python': _IMPORT_PYTHON_RE,
        'javascript': _IMPORT_JS_RE,
        'rust': _IMPORT_RUST_RE,
        'c': _IMPORT_C_RE,
        'php': _IMPORT_PHP_RE,
        'go': _IMPORT_GO_RE,
    }


def get_url_regexes() -> Dict[str, Pattern]:
    """Return dict of URL extraction patterns."""
    return {
        'http': _URL_HTTP_RE,
        'markdown': _URL_MARKDOWN_LINK_RE,
        'html': _URL_HTML_HREF_RE,
    }


def get_issue_ref_regexes() -> List[Pattern]:
    """Return list of issue reference patterns."""
    return [_ISSUE_REF_RE, _ISSUE_URL_RE]


def get_version_regex() -> Pattern:
    """Return semantic version extraction pattern."""
    return _VERSION_SEMVER_RE


def get_email_regex() -> Pattern:
    """Return email extraction pattern."""
    return _EMAIL_RE


def get_code_lang_regex() -> Pattern:
    """Return code fence language hint pattern."""
    return _CODE_FENCE_LANG_RE
