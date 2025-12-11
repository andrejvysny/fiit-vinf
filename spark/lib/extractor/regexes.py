"""Compiled regex patterns for entity extraction.

All patterns are pre-compiled for performance. This module uses only stdlib `re`.
"""

import re
from typing import Dict, List, Pattern

# ============================================================================
# HTML Cleaning Patterns
# ============================================================================

_HTML_CHROME_PATTERNS: List[Pattern] = [
    re.compile(r'<a[^>]*?href="#[^"]*?skip[^"]*?"[^>]*?>.*?</a>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<header[^>]*?class="[^"]*?(?:Header|AppHeader|header)[^"]*?"[^>]*?>.*?</header>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<nav[^>]*?(?:class="[^"]*?(?:UnderlineNav|navigation|nav-bar)[^"]*?"|role="navigation")[^>]*?>.*?</nav>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<(?:div|aside)[^>]*?class="[^"]*?(?:sidebar|Sidebar)[^"]*?"[^>]*?>.*?</(?:div|aside)>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<footer[^>]*?>.*?</footer>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<nav[^>]*?aria-label="[^"]*?[Bb]readcrumb[^"]*?"[^>]*?>.*?</nav>', re.IGNORECASE | re.DOTALL),
    re.compile(r'<div[^>]*?class="[^"]*?(?:command-bar|filter-bar|CommandBar)[^"]*?"[^>]*?>.*?</div>', re.IGNORECASE | re.DOTALL),
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

_STAR_COUNTER_RE = re.compile(
    r'<span\s+id="repo-stars-counter-star"[^>]*?title="([0-9,]+)"',
    re.IGNORECASE | re.DOTALL
)
_STAR_ARIA_RE = re.compile(
    r'aria-label="(\d+(?:,\d+)*)\s+users?\s+starred',
    re.IGNORECASE
)
_FORK_COUNTER_RE = re.compile(
    r'<span\s+id="repo-network-counter"[^>]*?title="([0-9,]+)"',
    re.IGNORECASE | re.DOTALL
)
_LANG_NAME_RE = re.compile(
    r'<span\s+[^>]*?itemprop="programmingLanguage"[^>]*?>([^<]+)</span>',
    re.IGNORECASE | re.DOTALL
)
_LANG_PERCENT_RE = re.compile(r'(\d{1,3}(?:\.\d+)?)\s*%', re.MULTILINE)
_LANG_STATS_BLOCK_RE = re.compile(
    r'<div[^>]*?(?:class="[^"]*?language[^"]*?"|itemprop="programmingLanguage")[^>]*?>.*?</div>',
    re.IGNORECASE | re.DOTALL
)
_README_ARTICLE_RE = re.compile(
    r'<article[^>]*?(?:class="[^"]*?markdown-body[^"]*?"|itemprop="text"|data-[^>]*?readme)[^>]*?>(.*?)</article>',
    re.IGNORECASE | re.DOTALL
)
_README_JSON_SCRIPT_RE = re.compile(
    r'<script\s+[^>]*?type="application/json"[^>]*?>(.*?)</script>',
    re.IGNORECASE | re.DOTALL
)
_ABOUT_HEADER_RE = re.compile(
    r'<h2[^>]*?>\s*About\s*</h2>(.*?)(?=<h[12]|<div[^>]*?class="[^"]*?(?:sidebar|footer)[^"]*?")',
    re.IGNORECASE | re.DOTALL
)
_LICENSE_SPDX_RE = re.compile(
    r'\b(MIT|Apache-2\.0|Apache-1\.1|GPL-3\.0|GPL-2\.0|LGPL-3\.0|LGPL-2\.1|'
    r'BSD-3-Clause|BSD-2-Clause|MPL-2\.0|ISC|CC0-1\.0|Unlicense|'
    r'AGPL-3\.0|EPL-2\.0|EPL-1\.0|CC-BY-4\.0|CC-BY-SA-4\.0)\b',
    re.IGNORECASE
)
_LICENSE_LINK_RE = re.compile(
    r'<a\s+[^>]*?href="[^"]*?(?:license|LICENSE)[^"]*?"[^>]*?>(.*?)</a>',
    re.IGNORECASE | re.DOTALL
)
_TOPIC_TAG_RE = re.compile(
    r'<a\s+[^>]*?(?:class="[^"]*?topic-tag[^"]*?"|data-octo-click="topic")[^>]*?>([\w\-]+)</a>',
    re.IGNORECASE | re.DOTALL
)
_URL_HTTP_RE = re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)
_URL_MARKDOWN_LINK_RE = re.compile(r'\[([^\]]+)\]\(([^\)]+)\)', re.MULTILINE)
_URL_HTML_HREF_RE = re.compile(
    r'<a\s+[^>]*?href=["\']([^"\']+)["\']',
    re.IGNORECASE | re.DOTALL
)
_EMAIL_RE = re.compile(
    r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b',
    re.MULTILINE
)

# ============================================================================
# Pattern Accessor Functions
# ============================================================================

def get_html_chrome_patterns() -> List[Pattern]:
    return _HTML_CHROME_PATTERNS

def get_html_remove_tags_regex() -> Pattern:
    return _HTML_REMOVE_TAGS_RE

def get_html_self_closing_regex() -> Pattern:
    return _HTML_SELF_CLOSING_RE

def get_html_comment_regex() -> Pattern:
    return _HTML_COMMENT_RE

def get_html_doctype_regex() -> Pattern:
    return _HTML_DOCTYPE_RE

def get_html_block_tags_regex() -> Pattern:
    return _HTML_BLOCK_TAGS_RE

def get_html_br_hr_regex() -> Pattern:
    return _HTML_BR_HR_RE

def get_html_generic_tag_regex() -> Pattern:
    return _HTML_TAG_RE

def get_html_multi_space_regex() -> Pattern:
    return _HTML_MULTI_SPACE_RE

def get_html_around_newline_regex() -> Pattern:
    return _HTML_AROUND_NEWLINE_RE

def get_html_multi_newline_regex() -> Pattern:
    return _HTML_MULTI_NEWLINE_RE

def get_star_regexes() -> List[Pattern]:
    return [_STAR_COUNTER_RE, _STAR_ARIA_RE]

def get_fork_regexes() -> List[Pattern]:
    return [_FORK_COUNTER_RE]

def get_lang_stats_regexes() -> Dict[str, Pattern]:
    return {'name': _LANG_NAME_RE, 'percent': _LANG_PERCENT_RE, 'block': _LANG_STATS_BLOCK_RE}

def get_readme_regexes() -> Dict[str, Pattern]:
    return {'article': _README_ARTICLE_RE, 'json_script': _README_JSON_SCRIPT_RE, 'about': _ABOUT_HEADER_RE}

def get_license_regexes() -> List[Pattern]:
    return [_LICENSE_SPDX_RE, _LICENSE_LINK_RE]

def get_topic_regex() -> Pattern:
    return _TOPIC_TAG_RE

def get_url_regexes() -> Dict[str, Pattern]:
    return {'http': _URL_HTTP_RE, 'markdown': _URL_MARKDOWN_LINK_RE, 'html': _URL_HTML_HREF_RE}

def get_email_regex() -> Pattern:
    return _EMAIL_RE
