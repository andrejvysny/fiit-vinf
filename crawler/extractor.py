import html
import logging
import re
from dataclasses import dataclass
from typing import List, Tuple
from urllib.parse import urljoin, urlsplit

logger = logging.getLogger(__name__)


@dataclass
class TextExtractionResult:
    title: str
    text: str


@dataclass
class EntityMatch:
    entity_type: str
    value: str
    start: int
    end: int
    matched: str


class HtmlTextExtractor:
    """Regex-only HTML → plain-text extractor tuned for GitHub pages."""

    _DROP_ELEMENTS_RE = re.compile(
        r"(?is)<(script|style|iframe|canvas|svg|noscript|template)[^>]*>.*?</\1>"
    )
    _COMMENT_RE = re.compile(r"<!--.*?-->", re.DOTALL)
    _DOCTYPE_RE = re.compile(r"<!DOCTYPE.*?>", re.IGNORECASE | re.DOTALL)
    _BLOCK_BREAK_RE = re.compile(
        r"</?(?:p|div|section|article|aside|header|footer|main|nav|h[1-6]|pre|blockquote|li|ul|ol|table|tr|thead|tbody|tfoot|th|td|dl|dt|dd)[^>]*>",
        re.IGNORECASE,
    )
    _BR_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
    _HR_RE = re.compile(r"<hr\s*/?>", re.IGNORECASE)
    _TITLE_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
    _TAG_RE = re.compile(r"<[^>]+>")
    _MULTI_SPACE_RE = re.compile(r"[ \t\f\v]+")
    _AROUND_NEWLINE_SPACE_RE = re.compile(r"[ \t\f\v]*\n[ \t\f\v]*")
    _MULTI_NEWLINE_RE = re.compile(r"\n{3,}")

    def extract(self, html_content: str) -> TextExtractionResult:
        if not html_content:
            return TextExtractionResult(title="", text="")

        working = html_content
        title = ""

        title_match = self._TITLE_RE.search(working)
        if title_match:
            title_raw = html.unescape(title_match.group(1).strip())
            title = self._MULTI_SPACE_RE.sub(" ", title_raw)

        # Remove non-content elements and comments
        working = self._DROP_ELEMENTS_RE.sub(" ", working)
        working = self._COMMENT_RE.sub(" ", working)
        working = self._DOCTYPE_RE.sub(" ", working)

        # Swap structural tags for newlines before removing the rest
        working = self._BR_RE.sub("\n", working)
        working = self._HR_RE.sub("\n", working)
        working = self._BLOCK_BREAK_RE.sub("\n", working)

        # Strip remaining tags
        working = self._TAG_RE.sub(" ", working)

        # Decode HTML entities and normalise whitespace
        working = html.unescape(working)
        working = working.replace("\xa0", " ")
        working = working.replace("\r\n", "\n").replace("\r", "\n")

        # Collapse spaces but keep paragraph boundaries
        working = self._MULTI_SPACE_RE.sub(" ", working)
        working = self._AROUND_NEWLINE_SPACE_RE.sub("\n", working)
        working = self._MULTI_NEWLINE_RE.sub("\n\n", working)

        # Final trim and removal of empty lines
        lines = [line.strip() for line in working.split("\n")]
        cleaned_lines: List[str] = [line for line in lines if line]
        text = "\n".join(cleaned_lines).strip()

        return TextExtractionResult(title=title, text=text)

    def extract_text(self, html_content: str) -> str:
        return self.extract(html_content).text


class RegexEntityExtractor:
    """Entity extraction using only regex patterns."""

    def __init__(self) -> None:
        self._patterns: List[Tuple[str, re.Pattern, str]] = [
            (
                "LICENSE",
                re.compile(r"(?i)\b(?P<value>(?:mit|apache|bsd|gpl|lgpl|agpl)\s+license)\b"),
                "value",
            ),
            (
                "LANG",
                re.compile(r"```(?P<value>[a-z0-9+\-#]+)\b"),
                "value",
            ),
            (
                "IMPORT",
                re.compile(
                    r"(?m)^\s*import\s+[^;]+?\s+from\s+['\"](?P<value>[^'\"]+)['\"]"
                ),
                "value",
            ),
            (
                "IMPORT",
                re.compile(r"(?m)^\s*require\(['\"](?P<value>[^'\"]+)['\"]\)"),
                "value",
            ),
            (
                "IMPORT",
                re.compile(r"(?m)^\s*#include\s*[<\"](?P<value>[^>\"\s]+)[>\"]"),
                "value",
            ),
            (
                "URL",
                re.compile(r"(?i)<a[^>]+?href\s*=\s*['\"](?P<value>[^'\"]+)['\"][^>]*>"),
                "value",
            ),
            (
                "URL",
                re.compile(r"\[(?:[^\]]+)\]\((?P<value>https?://[^\s)]+)\)"),
                "value",
            ),
        ]

    def extract(self, text: str) -> List[EntityMatch]:
        if not text:
            return []

        matches: List[EntityMatch] = []
        for entity_type, pattern, group_name in self._patterns:
            for match in pattern.finditer(text):
                if group_name in match.re.groupindex:
                    value = match.group(group_name)
                    start, end = match.span(group_name)
                else:
                    value = match.group(0)
                    start, end = match.span(0)
                if not value:
                    continue

                value = value.strip()

                if entity_type == "URL" and not value.lower().startswith(
                    ("http://", "https://")
                ):
                    continue

                matches.append(
                    EntityMatch(
                        entity_type=entity_type,
                        value=value,
                        start=start,
                        end=end,
                        matched=match.group(0).strip(),
                    )
                )

        return matches


class LinkExtractor:
    
    def extract(self, html_content: str, base_url: str) -> List[str]:
        if not html_content or not base_url:
            return []
        
        try:
            # Regex pattern to match href attributes in <a> tags
            # Handles both quoted (single/double) and unquoted href values
            pattern = r"""<a\s+[^>]*?href\s*=\s*(?:["']([^"']+)["']|([^\s>]+))"""
            
            seen = set()
            results = []
            
            for match in re.finditer(pattern, html_content, re.IGNORECASE | re.DOTALL):
                # Extract href value from either capture group (quoted or unquoted)
                href = match.group(1) or match.group(2)
                
                # Unescape HTML entities (e.g., &amp; -> &)
                href = html.unescape(href).strip()
                
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                
                if href.startswith("mailto:") or href.startswith("tel:"):
                    continue
                absolute_url = urljoin(base_url, href)
                
                if not self._is_supported_scheme(absolute_url):
                    continue

                url_without_fragment = self._remove_fragment(absolute_url)

                # Track unique URLs while preserving appearance order
                if url_without_fragment not in seen:
                    seen.add(url_without_fragment)
                    results.append(url_without_fragment)
            
            logger.debug(f"Extracted {len(results)} links from {base_url}")
            return results
            
        except Exception as e:
            logger.error(f"Error extracting links from {base_url}: {e}")
            return []
    
    def _is_supported_scheme(self, url: str) -> bool:
        """Check whether URL uses a supported scheme (HTTP or HTTPS)."""
        try:
            parts = urlsplit(url)
            return parts.scheme in ("http", "https") and bool(parts.netloc)
        except Exception:
            return False
    
    def _remove_fragment(self, url: str) -> str:
        try:
            parts = urlsplit(url)
            from urllib.parse import urlunsplit
            return urlunsplit((parts.scheme, parts.netloc, parts.path, parts.query, ""))
        except Exception:
            return url
